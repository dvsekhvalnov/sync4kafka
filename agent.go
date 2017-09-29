package agent

import (
	"github.com/Shopify/Sarama"
	"github.com/dvsekhvalnov/sync4kafka/utils"
	"time"
)

type globals struct {
	cfg    *Config
	server *Server
}

type MetadataChanges struct {
	Added   []PartitionInfo
	Removed []PartitionInfo
}

func (ctx *globals) openBroker() (*sarama.Broker, error) {
	err := ctx.server.broker.Open(ctx.cfg.Config)
	return ctx.server.broker, err
}

func Sync(cfg *Config) <-chan *sarama.ConsumerMessage {
	ticker := time.NewTicker(time.Millisecond * time.Duration(cfg.MetadataRefreshMs))

	cfg.Version = sarama.V0_10_2_0

	// server :=

	ctx := &globals{
		server: DiscoverServer(cfg.BrokerUrl, cfg),
		cfg:    cfg,
	}

	consumerChannel := Consume(Diff(Refresh(ticker.C, ctx), ctx), ctx)

	if cfg.AutotrackOffsets {
		//exclude system offsests topic, otherwise we end up in dead loop
		//commit offset will change __consumer_offsets, which in turn will be detected as new message
		cfg.Exclude("__consumer_offsets")

		//use Kafka offsets management strategy
		cfg.FetchOffsets = func(partition *PartitionInfo) int64 {
			return FetchOffsets(ctx, partition)
		}
		return TrackOffsets(consumerChannel, ctx)
	} else {

		if cfg.FetchOffsets == nil {
			//Default offset management if not provided (no offset management)
			cfg.FetchOffsets = func(partition *PartitionInfo) int64 {
				return -1
			}
		}

		return consumerChannel
	}
}

func TrackOffsets(in <-chan *sarama.ConsumerMessage, ctx *globals) <-chan *sarama.ConsumerMessage {
	out := make(chan *sarama.ConsumerMessage)

	go func() {
		defer close(out)

		for msg := range in {
			out <- msg
			//TODO: probably commit in batch async, based on number of uncommited offsets or time ticks
			Log.Printf("Commiting offset=%v, topic=%v, partition=%v\n", msg.Offset, msg.Topic, msg.Partition)
			MarkOffsets(ctx, msg.Offset+1, msg.Topic, msg.Partition)
		}
	}()

	return out
}

func consumePartition(ctx *globals, consumer sarama.Consumer, partition PartitionInfo, out chan *sarama.ConsumerMessage) chan bool {
	quit := make(chan bool) //signal channel for cancellation

	go func() {

		defer close(quit)

		Log.Printf("Starting partition consumer, topic=%v, partition=%v\n", partition.Topic, partition.ID)

		offset := ctx.cfg.FetchOffsets(&partition)
		Log.Printf("Fetched offset=%v, topic=%v, partition=%v\n", offset, partition.Topic, partition.ID)

		//consume everything if no previous offset found
		if offset < 0 {
			offset = sarama.OffsetOldest
		}

		if partitionConsumer, err := consumer.ConsumePartition(partition.Topic, partition.ID, offset); err == nil {
			//TODO: check errors?
			// fmt.Printf("Starting parition consumer, topic=%v, partition=%v\n", partition.Topic, partition.Metadata.ID)

			for {

				select {
				case msg := <-partitionConsumer.Messages():

					if msg != nil {
						out <- msg
					}
				case consumerError := <-partitionConsumer.Errors():
					Log.Printf("[ERROR]: %v", consumerError)

					if consumerError == nil {
						Log.Println("Out of sync, re-init consumer")

						//somehow we run out of sync, re-init consumer
						partitionConsumer, err = consumer.ConsumePartition(partition.Topic, partition.ID, sarama.OffsetOldest)
					}
				case <-quit:
					Log.Printf("Closing partition consumer topic=%v, partition=%v\n", partition.Topic, partition.ID)
					if err := partitionConsumer.Close(); err != nil {
						Log.Println("there were problems ", err)
					} else {
						Log.Print("Partition consumer topic=%v, partition=%v was closed successully.\n")
					}

					quit <- true

					return
				}
			}

		} else {
			//TODO: should we retry or what?
			Log.Printf("There were errors starting consumer, topic=%v, partition=%v\n", partition.Topic, partition.ID)
		}
	}()

	return quit
}

func Consume(in <-chan *MetadataChanges, ctx *globals) <-chan *sarama.ConsumerMessage {

	out := make(chan *sarama.ConsumerMessage)

	go func() {
		defer close(out)

		Log.Println("Initializing Kafka client.")

		var consumers = make(map[PartitionInfo]chan bool)

		consumer, err := sarama.NewConsumer([]string{ctx.server.broker.Addr()}, ctx.cfg.Config)

		//TODO: check some code from sarama itself to handle errors
		if err != nil {
			Log.Printf("Err=%v\n", err)
			panic(err)
		}

		defer consumer.Close()

		for changes := range in {
			Log.Println("Processing metadata changes.")

			for _, partition := range changes.Added {
				//start consuming in-parallel
				consumers[partition] = consumePartition(ctx, consumer, partition, out)
			}

			for _, partition := range changes.Removed {
				//stop consuming, log something
				Log.Printf("Closing consumer: topic=%v, partition=%v\n", partition.Topic, partition.ID)

				if paritionConsumer, ok := consumers[partition]; ok {
					paritionConsumer <- true //send cancel signal
					<-paritionConsumer       //wait cancel to complete
					Log.Println("Consumer closed.")
					delete(consumers, partition)
				} else {
					Log.Printf("No active consumer found for topic=%v, partition=%v, skipping.", partition.Topic, partition.ID)
				}
			}
		}
	}()

	return out
}

func indexPartitions(partitions []KafkaPartition, cfg *Config) map[PartitionInfo]*KafkaPartition {
	result := make(map[PartitionInfo]*KafkaPartition, len(partitions))

	for i, partition := range partitions {

		//process exclude filters
		if len(cfg.excludeTopics) > 0 && utils.AnyMatches(cfg.excludeTopics, partition.Topic) {
			continue
		}

		//process includes filters
		if len(cfg.includeTopics) > 0 && !utils.AnyMatches(cfg.includeTopics, partition.Topic) {
			continue
		}

		result[PartitionInfo{partition.Topic, partition.Metadata.ID}] = &partitions[i]
	}

	return result
}

func Diff(in <-chan *Server, ctx *globals) <-chan *MetadataChanges {
	out := make(chan *MetadataChanges)

	go func() {
		defer close(out)

		var state = make(map[PartitionInfo]*KafkaPartition)

		for meta := range in {

			found := indexPartitions(meta.Partitions(), ctx.cfg)

			changes := &MetadataChanges{
				Added:   make([]PartitionInfo, 0),
				Removed: make([]PartitionInfo, 0),
			}

			// [newParitions - state] = newly added partitions
			for key, partition := range found {

				//new partition found
				if _, ok := state[key]; !ok {
					changes.Added = append(changes.Added, key)
				}

				//refresh metadata
				state[key] = partition
			}

			// [state - newParitions] = remove partitions
			for key, _ := range state {
				//partition is no longer exists
				if _, ok := found[key]; !ok {
					changes.Removed = append(changes.Removed, key)
					delete(state, key)
				}
			}

			// post changes if any
			if len(changes.Added) > 0 || len(changes.Removed) > 0 {
				Log.Println("Sending metadata changes")
				out <- changes
			}

		}
	}()

	return out
}

func Refresh(in <-chan time.Time, ctx *globals) <-chan *Server {
	out := make(chan *Server)

	go func() {
		defer close(out)

		var newBroker *Server = ctx.server
		for _ = range in {
			newBroker = RefreshMetadata(ctx)
			out <- newBroker
		}
	}()

	return out
}
