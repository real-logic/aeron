# Aeron Samples

Here you will find a collection of samples and tools for Aeron. The build process builds a complete
jar of the samples and places it in this location

     aeron-samples/build/libs/samples.jar

Each of the Samples can be run with a simple script that can be found in:

    aeron-samples/scripts/

Before running any of the samples below the media driver needs to be launched by running one of:

    aeron-samples/scripts/media-driver
    aeron-samples/scripts/low-latency-media-driver
    
Here is a brief list of the samples and what each tries to do:

- __BasicSubscriber__: Simple subscriber that prints the contents of messages it receives.
- __BasicPublisher__: Simple publisher that sends a number of messages with a one second pause between them.
- __RateSubscriber__: Subscriber that prints the rate of reception of messages.
- __StreamingPublisher__: Publisher that streams out messages as fast as possible, displaying rate of publication.
- __Ping__: Ping side of Ping/Pong latency testing tool.
- __Pong__: Pong side of Ping/Pong latency testing tool.

Here is a brief list of monitoring and diagnostic tools:

- __AeronStat__: Monitoring tool that prints the labels and values of the counters in use by a media driver.
- __ErrorStat__: Monitoring tool that prints the distinct errors observed by the media driver.
- __LossStat__: Monitoring tool that prints a report of loss recorded by stream.
- __LogInspector__: Diagnostic tool that prints out the contents of a log buffer for a given stream for debugging.

Also included is some performance tests that can run all in the same process for convenience without a media driver,
 or across processes for illustration, with variants for throughput or latency measurement:

- __embedded__: Tests tend to run in the same process.
- __media__: Variants for IPC using shared memory or UDP via the network.

## Aeron Archive Samples

In the [archive](https://github.com/real-logic/aeron/tree/master/aeron-samples/scripts/archive) sub-directory, 
or package, you can find samples for recording and replay of streams from an Archive.

    aeron-samples/scripts/archive/
