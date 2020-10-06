# sync4kafka
This is simple synchronization library that connects to single kafka broker and
syncs all messages from all topics / partitions that broker is leader for into outbound go channel.

It keeps dynamically listen to broker metadata updates, picks up or drops re-assigned partitions,
and keep consuming all relevant messages.

Primarily developed for https://github.com/dvsekhvalnov/k-ray and unlikely can be very useful
outside of it.

## Status
Experimental. Born from hackathon project. Not really follows any go best practices and patterns for channels.

