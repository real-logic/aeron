# Usage Guide

There are examples of nearly all the public C Archive Client APIs being used in the unit test file at `aeron-archive/src/test/c/client/aeron_archive_test.cpp`.

### Establishing a connection to an archive

#### Synchronous connect

```
aeron_archive_context_t *ctx;
aeron_archive_t *archive = NULL;

aeron_archive_context_init(&ctx);
aeron_archive_connect(&archive, ctx);
aeron_archive_context_close(ctx);
```

The archive context passed into connect is immediately copied, and so it can be closed immediately after connect returns.

#### Asynchronous connect

```
aeron_archive_context_t *ctx;
aeron_archive_async_connect_t *async;
aeron_archive_t *archive = NULL;

aeron_archive_context_init(&ctx);
aeron_archive_async_connect(&async, ctx);
aeron_archive_context_close(ctx);

while (NULL == archive)
{
    idle();

    aeron_archive_async_connect_poll(&archive, async);
}
```

The `connect_poll` call will set the archive pointer once the connection is complete.

### Closing a connection

```
aeron_archive_close(archive);
```

### Recording

Starting and stopping:
```
int64_t subscription_id;

aeron_archive_start_recording(
    &subscription_id,
    archive,                                // an aeron_archive_t
    "aeron:udp?endpoint=localhost:3333",    // channel to record
    1234,                                   // stream id to record
    AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
    false);
...

aeron_archive_stop_recording_subscription(
    archive,
    subscription_id);
```

### List Recordings

First, define a recording descriptor consumer callback:
```
void recording_descriptor_consumer(
    aeron_archive_recording_descriptor_t *descriptor,
    void *clientd)
{
    void *my_data = clientd;

    descriptor->recording_id;
}
```

List some number of recordings:
```
int32_t count;
void *my_data

aeron_archive_list_recordings(
    &count,                         // will be set to the number of descriptors found
    archive,
    initial_recording_id,           // recording id at which to start the listing
    max_record_count,               // the max number of recordings to list
    recording_descriptor_consumer,
    my_data);
```

List recordings that match a pattern:
```
aeron_archive_list_recordings_for_uri(
    &count,
    archive,
    initial_recording_id,
    max_record_count,
    "aeron:udp",                    // a string fragment to match against
    stream_id,                      // a stream id to match against
    recording_descriptor_consumer,
    my_data);
```

List a single recording using a recording id:
```

aeron_archive_list_recording(
    &count,
    archive,
    recording_id,                   // the recording id to list
    recording_descriptor_consumer,
    my_data);
```

### Replay

First, initialize some replay parameters:
```
aeron_archive_replay_params_t replay_params;
aeron_archive_replay_params_init(&replay_params);

replay_params.position = 0;                  // the position in the recording at which to start the replay
replay_params.length = stop_position;
replay_params.file_io_max_length = 4096;
```

Starting and stopping:
```
int64_t replay_session_id;
aeron_archive_start_replay(
    &replay_session_id,        // will be set to the replay session id
    archive,
    recording_id,              // the recording id indicating the recording to replay
    my_channel,                // the channel onto which the replay should be published
    my_stream_id,              // the stream onto which the replay should be published
    &replay_params);

...

aeron_archive_stop_replay(archive, replay_session_id));
```
