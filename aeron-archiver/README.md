The aeron-archiver is an Aeron client application which enables
published data persistance and replay support. Currently implemented
fuctionality is limited to the following:
- Archive: service can record a particular subscription, described
by <__channel,streamId__>. Each resulting image for the subscription
will be recorded under a new __stream instance id__.

- Replay: service can replay a recorded __stream instance id__ from
a particular __termId+termOffset__, and for a particular length.

- Query: service provides a rudimentary query interface which
allows __stream instance id__ discovery and description.

Protocol
=====
Messages are specified using SBE under ../aeron-archiver-codecs. The
Archiver communicates via the following interfaces:
 - Notifications channel: other parties can subscribe to the notifications
 to track the creation/termination/progress of archives. These are the
 notification messages specified in the codec.
 - Requests channel: this allows clients to initiate replay or queries
 conversations with the archiver. Conversations have a conversationId sent
 on the intiating request. The conversationId is expected to be managed by
 the clients and is offered as a means for clients to track multiple
 concurrent conversations. A conversation will typically involve the
 archiver sending data back on a reply channel specified by the client.

Notifications
----

Start/Stop Archive Interaction 
----

Start/Abort Archive Replay Interaction 
----

Query Archive Descriptors
----

Persisted Format
=====
The Archiver is backed by 3 file types, all of which are expected to reside in the __archiver folder__.

 -  Index (one per archive folder): The index contains fixed size (4k) records of archive descriptors. The descriptors
 can be queried as described above. Each descriptor is 4k aligned, and the __stream instance id__
 is a simple sequence, which means lookup is a straight dead reconning operation. See the codec
 fo full descriptor details.
 - Archive Metadata (one per stream instance): This is a duplicate of the data kept in the index, but the file
 is memory mapped and updated on the go while recording.
 - Archive Data (many per stream instance): This is where the recorded data is kept.
 
 Usage
 ===
 
 