package uk.co.real_logic.aeron.tools;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.ParseException;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.InactiveConnectionHandler;
import uk.co.real_logic.aeron.NewConnectionHandler;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.BackoffIdleStrategy;
import uk.co.real_logic.aeron.common.IdleStrategy;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.tools.TLRandom.SeedCallback;
import uk.co.real_logic.agrona.DirectBuffer;

public class SubscriberTool
	implements RateReporter.Stats, SeedCallback
{
	private boolean shuttingDown;
	private PubSubOptions options = new PubSubOptions();
	private SubscriberThread subscribers[];

    public static void main(String[] args)
    {
    	SubscriberTool subTool = new SubscriberTool();
    	try
    	{
			if (1 == subTool.options.parseArgs(args))
			{
				subTool.options.printHelp("SubscriberTool");
				System.exit(-1);
			}
		}
    	catch (ParseException e)
    	{
    		System.out.println(e.getMessage());
			subTool.options.printHelp("SubscriberTool");
			System.exit(-1);
		}

    	sanityCheckOptions(subTool.options);

    	/* Set pRNG seed callback. */
    	TLRandom.setSeedCallback(subTool);

    	/* Shut down gracefully when we receive SIGINT. */
    	SigInt.register(() -> subTool.shuttingDown = true);

    	/* Start embedded driver if requested. */
    	MediaDriver driver = null;
    	if (subTool.options.getUseEmbeddedDriver())
    	{
    		driver = MediaDriver.launch();
    	}

    	/* Create and start receiving threads. */
    	Thread subThreads[] = new Thread[subTool.options.getThreads()];
    	subTool.subscribers = new SubscriberThread[subTool.options.getThreads()];
    	for (int i = 0; i < subTool.options.getThreads(); i++)
    	{
    		subTool.subscribers[i] = subTool.new SubscriberThread(i);
    		subThreads[i] = new Thread(subTool.subscribers[i]);
    		subThreads[i].start();
    	}

        RateReporter rateReporter = new RateReporter(subTool);

        /* Wait for threads to exit. */
        try
        {
        	for (int i = 0; i < subThreads.length; i++)
        	{
        		subThreads[i].join();
        	}
        	rateReporter.close();
        }
        catch (InterruptedException e)
        {
        	e.printStackTrace();
        }

        /* Close the driver if we had opened it. */
        if (subTool.options.getUseEmbeddedDriver())
    	{
    		driver.close();
    	}

        try
        {
        	/* Close any open output files. */
			subTool.options.close();
		}
        catch (IOException e)
        {
			e.printStackTrace();
		}

        final long verifiableMessages = subTool.verifiableMessages();
        final long nonVerifiableMessages = subTool.nonVerifiableMessages();
        final long bytesReceived = subTool.bytes();
        System.out.format("Exiting. Received %d messages (%d bytes) total. %d verifiable and %d non-verifiable.%n",
        		verifiableMessages + nonVerifiableMessages,
        		bytesReceived, verifiableMessages, nonVerifiableMessages);
    }

    /** Warn about options settings that might cause trouble. */
    private static void sanityCheckOptions(PubSubOptions options)
    {
    	if (options.getThreads() > 1)
    	{
    		if (options.getOutput() != null)
    		{
    			System.out.println("WARNING: File output may be non-deterministic when multiple subscriber threads are used.");
    		}
    	}
    }

    /**
     * A snapshot (non-atomic) total of the number of verifiable messages
     * received across all receiving threads.
     * @return current total number of verifiable messages received
     */
    @Override
	public long verifiableMessages()
    {
    	long totalMessages = 0;
    	for (int i = 0; i < subscribers.length; i++)
    	{
    		totalMessages += subscribers[i].verifiableMessagesReceived();
    	}
    	return totalMessages;
    }

    /**
     * A snapshot (non-atomic) total of the number of bytes
     * received across all receiving threads.
     * @return current total number of bytes received
     */
    @Override
	public long bytes()
    {
    	long totalBytes = 0;
    	for (int i = 0; i < subscribers.length; i++)
    	{
    		totalBytes += subscribers[i].bytesReceived();
    	}
    	return totalBytes;
    }

    /**
     * A snapshot (non-atomic) total of the number of non-verifiable messages
     * received across all receiving threads.
     * @return current total number of non-verifiable messages received
     */
    @Override
	public long nonVerifiableMessages()
    {
    	long totalMessages = 0;
    	for (int i = 0; i < subscribers.length; i++)
    	{
    		totalMessages += subscribers[i].nonVerifiableMessagesReceived();
    	}
    	return totalMessages;
    }

	/**
	 * Optionally sets the random seed used for the TLRandom class, and reports the seed used.
	 * @return the seed to use
	 */
	@Override
	public long setSeed(long seed)
	{
		if (options.getRandomSeed() != 0)
		{
			seed = options.getRandomSeed();
		}
		System.out.format("Thread %s using random seed %d.%n", Thread.currentThread().getName(), seed);
		return seed;
	}

	class SubscriberThread implements Runnable, InactiveConnectionHandler, NewConnectionHandler
	{
		final int threadId;
		private long nonVerifiableMessagesReceived;
		private long verifiableMessagesReceived;
		private long bytesReceived;
		private byte[] bytesToWrite = new byte[1];

		public SubscriberThread(int threadId)
		{
			this.threadId = threadId;
		}

		/** Get the number of bytes of all message types received so far by this
		 * individual subscriber thread.
		 * @return the number of bytes received by this thread
		 */
		public long bytesReceived()
		{
			return bytesReceived;
		}

		/**
		 * Gets the number of non-verifiable messages received by this individual subscriber thread.
		 * @return number of non-verifiable messages received by this thread
		 */
		public long nonVerifiableMessagesReceived()
		{
			return nonVerifiableMessagesReceived;
		}

		/**
		 * Gets the number of verifiable messages received by this individual subscriber thread.
		 * @return number of verifiable messages received by this thread
		 */
		public long verifiableMessagesReceived()
		{
			return verifiableMessagesReceived;
		}

		/**
		 * Implements the DataHandler callback for subscribers and checks
		 * any verifiable messages received.
		 *
		 */
		public class MessageStreamHandler
		{
			private final String channel;
			private final int streamId;
			private final MessageStream ms = new MessageStream();
			private final OutputStream os = options.getOutput();

			public MessageStreamHandler(String channel, int streamId)
			{
				this.channel = channel;
				this.streamId = streamId;
			}

			public void onMessage(DirectBuffer buffer, int offset, int length, Header header)
		    {
				bytesReceived += length;
		    	if (MessageStream.isVerifiable(buffer, offset))
		    	{
		    		verifiableMessagesReceived++;
		    		try
		    		{
						ms.putNext(buffer, offset, length);
					}
		    		catch (Exception e)
		    		{
		    			System.out.format("Channel %s:%d failed verifiable messages check:%n", channel, streamId);
						e.printStackTrace();
						System.exit(-1);
					}
		    	}
		    	else
		    	{
		    		nonVerifiableMessagesReceived++;
		    	}
		    	/* Write the message contents (minus verifiable message header) to our output
		    	 * stream if set. */
		    	if (os != null)
		    	{
		    		final int payloadOffset = ms.payloadOffset(buffer, offset);
		    		final int lengthToWrite = length - payloadOffset;
		    		if (lengthToWrite > bytesToWrite.length)
		    		{
		    			bytesToWrite = new byte[lengthToWrite];
		    		}
		    		buffer.getBytes(offset + payloadOffset, bytesToWrite, 0, lengthToWrite);
		    		try
		    		{
		    			os.write(bytesToWrite, 0, lengthToWrite);
		    		}
		    		catch (IOException e)
		    		{
						e.printStackTrace();
					}
		    	}
		    }
		}

		/** Subscriber thread.  Creates its own Aeron context, and subscribes
		 * on a round-robin'd subset of the channels and stream IDs configured.
		 */
		@Override
		public void run()
		{
			Thread.currentThread().setName("subscriber-" + threadId);

			/* Create a context and subscribe to what we're supposed to
			 * according to our thread ID. */
			@SuppressWarnings("resource")
			final Aeron.Context ctx = new Aeron.Context()
			.inactiveConnectionHandler(this)
			.newConnectionHandler(this)
			.errorHandler((throwable) ->
	        {
	            throwable.printStackTrace();
	            if (throwable instanceof DriverTimeoutException)
	            {
	                System.out.println("Driver does not appear to be running or has been unresponsive for ten seconds.");
	                System.exit(-1);
	            }
	        })
	        .mediaDriverTimeout(10000); /* ten seconds */

			final Aeron aeron = Aeron.connect(ctx);
			final ArrayList<Subscription> subscriptionsList = new ArrayList<Subscription>();

			int streamIdx = 0;
			for (int i = 0; i < options.getChannels().size(); i++)
			{
				ChannelDescriptor channel = options.getChannels().get(i);
				for (int j = 0; j < channel.getStreamIdentifiers().length; j++)
				{
					if ((streamIdx % options.getThreads()) == this.threadId)
					{
						System.out.format("%s subscribing to: %s#%d%n", Thread.currentThread().getName(),
								channel.getChannel(), channel.getStreamIdentifiers()[j]);

						DataHandler dataHandler =
								new FragmentAssemblyAdapter(
										new MessageStreamHandler(
												channel.getChannel(), channel.getStreamIdentifiers()[j])::onMessage);

						subscriptionsList.add(
								aeron.addSubscription(
										channel.getChannel(), channel.getStreamIdentifiers()[j], dataHandler));
					}
					streamIdx++;
				}
			}

			/* Poll the subscriptions. */
			final IdleStrategy idleStrategy = new BackoffIdleStrategy(
        			100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));
			final Subscription subscriptions[] = new Subscription[subscriptionsList.size()];
			subscriptionsList.toArray(subscriptions);
			while (!shuttingDown)
			{
				int fragmentsReceived = 0;
				for (int i = 0; i < subscriptions.length; i++)
				{
					fragmentsReceived += subscriptions[i].poll(1);
				}
				idleStrategy.idle(fragmentsReceived);
			}

			/* Shut down... */
			for (int i = 0; i < subscriptions.length; i++)
			{
				subscriptions[i].close();
			}
			aeron.close();
			ctx.close();
		}

		@Override
		public void onInactiveConnection(String channel, int streamId, int sessionId)
		{
			System.out.format("INACTIVE CONNECTION: channel \"%s\", stream %d, session %d%n", channel, streamId, sessionId);
		}

		@Override
		public void onNewConnection(String channel, int streamId, int sessionId,
				String sourceInformation)
		{
			System.out.format("NEW CONNECTION: channel \"%s\", stream %d, session %d, source \"%s\"%n",
					channel, streamId, sessionId, sourceInformation);
		}

	}
}
