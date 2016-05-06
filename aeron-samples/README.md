# Aeron Samples

Here you will find a collection of samples and tools for Aeron. The build process builds a complete
jar of the samples and places it in this location

     aeron-samples/build/libs/samples.jar

Each of the Samples can be run with a simple script that can be found in:

    aeron-samples/scripts/

Here is a brief list of the samples and what each tries to do.

- __BasicSubscriber__: Simple subscriber that prints the contents of messages it receives.
- __BasicPublisher__: Simple publisher that sends a number of messages with a one second pause between them.
- __RateSubscriber__: Subscriber that prints the rate of reception of messages.
- __StreamingPublisher__: Publisher that streams out messages as fast as possible, displaying rate of publication.
- __AeronStat__: Monitoring tool that prints the labels and values of the counters in use by a media driver.
- __ErrorStat__: Monitoring tool that prints the distinct errors observed by the media driver.
- __Ping__: Ping side of Ping/Pong latency testing tool.
- __Pong__: Pong side of Ping/Pong latency testing tool.