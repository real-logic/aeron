/**
 * Main Aeron API and processing logic
 *
 * Sender Example
 * <pre>
 * {@code
 * Aeron aeron = Aeron.newSingleIoDriver(null);
 *
 * Source source = aeron.newSource(new UDPDestination("172.16.29.29", 40123));
 * Channel aChannel = source.newChannel(10);
 * ByteBuffer buffer;
 *
 * // add data to buffer and get it ready to send
 * aChannel.send(buffer);
 * }
 * </pre>
 *
 * Receiver Example
 * <pre>
 * {@code
 * Aeron aeron = Aeron.newSingleIoDriver(null);
 *
 * Receiver.Builder builder = new Receiver.Builder()
 *     .destination(new UDPDestination("224.10.9.8", 40123))
 *     .channel(10, myChannel10Handler)
 *     .channel(20, myChannel20Handler)
 *     .events(myEventHandler)
 *
 * Receiver rcv = aeron.newReceiver(builder);
 * }
 * </pre>

 */
package uk.co.real_logic.aeron;