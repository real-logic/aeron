Under Construction, USE AT YOUR OWN RISK
===

The aeron-archiver is an application which enables data stream recording and replay support from an archive. 
Currently implemented functionality is limited to the following:
- Record: service can record a particular subscription, described
by <__channel,streamId__>. Each resulting image for the subscription
will be recorded under a new __recording id__.

- Replay: service can replay a recorded __recording id__ from
a particular __termId+termOffset__, and for a particular length.

- Query: service provides a rudimentary query interface which
allows __recording id__ discovery and description.

Protocol
=====
Messages are specified using SBE under ../aeron-archiver-codecs. The
Archiver communicates via the following interfaces:
 - Notifications channel: other parties can subscribe to the notifications
 to track the creation/termination/progress of archives. These are the
 notification messages specified in the codec.
 - Requests channel: this allows clients to initiate replay or queries
 conversations with the archiver. Conversations have a conversationId sent
 on the initiating request. The correlationId is expected to be managed by
 the clients and is offered as a means for clients to track multiple
 concurrent requests. A request will typically involve the
 archiver sending data back on a reply channel specified by the client.

Notifications
----

Start/Stop Recording Interaction 
----

Start/Abort Replay Interaction 
----

Query Recording Descriptors
----

Persisted Format
=====
The Archiver is backed by 3 file types, all of which are expected to reside in the __archiver dir__.

 -  Catalog (one per archive): The catalog contains fixed size (4k) records of archive descriptors. The 
 descriptors can be queried as described above. Each descriptor is 4k aligned, and the __recording id__
 is a simple sequence, which means lookup is a straight dead reconning operation. See the codec
 fo full descriptor details.
 - Recording Metadata (one per recorded stream): This is a duplicate of the data kept in the catalog, but the file
 is memory mapped and updated on the go while recording.
 - Recording Segment Data (many per recorded stream): This is where the recorded data is kept.
 
 Usage
 ===
 
 