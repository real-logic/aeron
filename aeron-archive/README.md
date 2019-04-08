Aeron Archive
===

[![Javadocs](http://www.javadoc.io/badge/io.aeron/aeron-all.svg)](http://www.javadoc.io/doc/io.aeron/aeron-all)

The aeron-archive is an module which enables Aeron data stream recording and replay from persistent storage. 

Samples can be found [here](https://github.com/real-logic/aeron/blob/master/aeron-samples/scripts/archive/README.md) and
systems tests [here](https://github.com/real-logic/aeron/tree/master/aeron-system-tests/src/test/java/io/aeron/archive).

Features:

- **Record:** service can record a particular subscription, described by `<channel, streamId>`. Each resulting image
for the subscription will be recorded under a new `recordingId`. Local network publications are recorded using the spy
feature for efficiency. If no subscribers are active then the recording can advance the stream by setting the
`aeron.spies.simulate.connection` system property to true.

- **Extend:** service can extend an existing recording by appending.

- **Replay:** service can replay a recorded `recordingId` from a particular `position`, and for a particular `length`
 which can be `Aeron.NULL_VALUE` for an open ended replay.

- **Query:** the catalog for existing recordings and the recorded position of an active recording.

- **Truncate:** allows a stopped recording to have its length truncated, and if truncated to the start position then it
is effectively deleted.

- **Replay Merge:** allows a late joining subscriber of a recorded stream to replay a recording and then merge with the
live stream for cut over if the consumer is fast enough to keep up.

Usage
=====

Protocol
=====
Messages are specified using SBE in [aeron-archive-codecs.xml](https://github.com/real-logic/aeron/blob/master/aeron-archive/src/main/resources/aeron-archive-codecs.xml).
The Archive communicates via the following interfaces:

 - **Recording Events stream:** other parties can subscribe to events for the start,
 stop, and progress of recordings. These are the recording events messages specified in the codec.
 
 - **Control Request stream:** this allows clients to initiate replay or queries interactions with the archive.
 Requests have a correlationId sent on the initiating request. The `correlationId` is expected to be managed by
 the clients and is offered as a means for clients to track multiple concurrent requests. A request will typically
 involve the archive sending data back on the reply channel specified by the client on the `ConnectRequest`.

A control session can be established with the Archive after a `ConnectRequest`. Operations happen within
the context of such a ControlSession which is allocated a `controlSessionId`.

Recording Events
----
Aeron clients wishing to observe the Archive recordings lifecycle can do so by subscribing to the recording events
channel. The messages are described in the codec. To fully capture the state of the Archive a client could subscribe
to these events as well as query for the full list of descriptors.

Persisted Format
=====
The Archive is backed by 2 file types, all of which are expected to reside in the `archiveDir`.

 -  **Catalog (one per archive):** The catalog contains fixed length (1k) records of recording
 descriptors. The descriptors can be queried as described above. Each descriptor entry is 1k aligned,
 and as the `recordingId` is a simple sequence, this means lookup is a dead reckoning operation.
 Each entry has a header (32 bytes) followed by the RecordingDescriptor, the header contains the encoded
 length of the RecordingDescriptor. See the codec schema for full descriptor details.
 
 - **Recording Segment Data (many per recorded stream):** This is where the recorded data is kept.
 Recording segments follow the naming convention of: `<recordingId>-<segmentIndex>.rec`
 The Archive copies data as is from the recorded Image. As such the files follow the same convention
 as Aeron data streams. Data starts at `startPosition`, which translates into the offset
 `startPosition % termBufferLength` in the first segment file. From there one can read fragments
 as described by the `DataHeaderFlyweight` up to the `stopPosition`. Segment length is a multiple of `termBufferLength`.
