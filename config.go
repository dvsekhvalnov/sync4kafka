package agent

import (
	"github.com/Shopify/Sarama"
	"github.com/gobwas/glob"
)

type Config struct {
	*sarama.Config

	// Local broker URL to connect to
	BrokerUrl string

	// How often refresh metadata about topics in ms
	MetadataRefreshMs int64

	// Optional. Broker advertised url to figure out local broker ID
	// if not provided, will be attempted to auto discovery based on existsting network
	// interfaces
	BrokerAdvertisedUrl string

	// Optional. Whether to commit & track consumed partitions offsets. When set to 'true',
	// will periodically commit position back to kafka __consumer_offsets topic.
	// Default is 'false'.
	AutotrackOffsets bool

	// Optional. consumer group name when AutotrackOffsets set to true
	ConsumerGroup string

	// Optional. Callback to retrive partition offset information if stored outside of Kafka
	// When AutotrackOffsets if used, the value is ignored and kafka based offset management is used
	// Default is no offset tracking, will re-consume all matching topics
	FetchOffsets func(partition *PartitionInfo) int64

	// Topics to include in sync. Glob patterns. If not provided will listen sync all
	// topics
	includeTopics []glob.Glob
	// Topics to exclude from sync. Glob patterns.
	excludeTopics []glob.Glob
}

func NewConfig() *Config {
	cfg := &Config{
		Config: sarama.NewConfig(),
	}

	cfg.Config.Consumer.Offsets.Initial = sarama.OffsetOldest

	return cfg
}

func (cfg *Config) Include(patterns ...string) *Config {

	if cfg.includeTopics == nil {
		cfg.includeTopics = make([]glob.Glob, 0)
	}

	for _, pattern := range patterns {
		cfg.includeTopics = append(cfg.includeTopics, glob.MustCompile(pattern))
	}

	return cfg
}

func (cfg *Config) Exclude(patterns ...string) *Config {
	if cfg.excludeTopics == nil {
		cfg.excludeTopics = make([]glob.Glob, 0)
	}

	for _, pattern := range patterns {
		cfg.excludeTopics = append(cfg.excludeTopics, glob.MustCompile(pattern))
	}

	return cfg
}
