# Aeron Examples

Here you will find a collection of examples and tools for Aeron. The build process builds a complete
jar of the examples and places it in this location

     aeron-examples/build/libs/examples.jar

Here is a brief list of the examples and what each tries to do.

- __ExampleSubscriber__: Simple subscriber that prints the contents of messages it receives.
- __ExamplePublisher__: Simple publisher that sends a number of messages with a one second pause between them.
- __RateSubscriber__: Subscriber that prints the rate of reception of messages.
- __StreamingPublisher__: Publisher that streams out messages as fast as possible, displaying rate of publication.
- __StatusInfo__: Monitoring application that prints the labels and values of the counters in use by a media driver.