/*
 * Copyright 2014-2018 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.*;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.LongHashSet;

import java.util.*;
import java.util.function.Consumer;

class SubscriptionLhsPadding
{
    @SuppressWarnings("unused")
    protected long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
}

class SubscriptionFields extends SubscriptionLhsPadding
{
    protected static final Image[] EMPTY_ARRAY = new Image[0];

    protected final long registrationId;
    protected int roundRobinIndex = 0;
    protected final int streamId;
    protected volatile boolean isClosed = false;

    protected volatile Image[] images = EMPTY_ARRAY;
    protected final LongHashSet imageIdSet = new LongHashSet();
    protected final ClientConductor conductor;
    protected final String channel;
    protected final AvailableImageHandler availableImageHandler;
    protected final UnavailableImageHandler unavailableImageHandler;
    protected int channelStatusId = 0;

    protected SubscriptionFields(
        final long registrationId,
        final int streamId,
        final ClientConductor clientConductor,
        final String channel,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        this.registrationId = registrationId;
        this.streamId = streamId;
        this.conductor = clientConductor;
        this.channel = channel;
        this.availableImageHandler = availableImageHandler;
        this.unavailableImageHandler = unavailableImageHandler;
    }
}

/**
 * Aeron Subscriber API for receiving a reconstructed {@link Image} for a stream of messages from publishers on
 * a given channel and streamId pair. {@link Image}s are aggregated under a {@link Subscription}.
 * <p>
 * {@link Subscription}s are created via an {@link Aeron} object, and received messages are delivered
 * to the {@link FragmentHandler}.
 * <p>
 * By default fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, whether or not they were fragmented, then the Subscriber
 * should be created with a {@link FragmentAssembler} or a custom implementation.
 * <p>
 * It is an application's responsibility to {@link #poll} the {@link Subscription} for new messages.
 * <p>
 * <b>Note:</b>Subscriptions are not threadsafe and should not be shared between subscribers.
 *
 * @see FragmentAssembler
 * @see ControlledFragmentHandler
 * @see Aeron#addSubscription(String, int)
 * @see Aeron#addSubscription(String, int, AvailableImageHandler, UnavailableImageHandler)
 */
public class Subscription extends SubscriptionFields implements AutoCloseable
{
    @SuppressWarnings("unused")
    protected long p16, p17, p18, p19, p20, p21, p22, p23, p24, p25, p26, p27, p28, p29, p30;

    Subscription(
        final ClientConductor conductor,
        final String channel,
        final int streamId,
        final long registrationId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        super(
            registrationId,
            streamId,
            conductor,
            channel,
            availableImageHandler,
            unavailableImageHandler);
    }

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    public String channel()
    {
        return channel;
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Return the registration id used to register this Subscription with the media driver.
     *
     * @return registration id
     */
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Callback used to indicate when an {@link Image} becomes available under this {@link Subscription}.
     *
     * @return callback used to indicate when an {@link Image} becomes available under this {@link Subscription}.
     */
    public AvailableImageHandler availableImageHandler()
    {
        return availableImageHandler;
    }

    /**
     * Callback used to indicate when an {@link Image} goes unavailable under this {@link Subscription}.
     *
     * @return Callback used to indicate when an {@link Image} goes unavailable under this {@link Subscription}.
     */
    public UnavailableImageHandler unavailableImageHandler()
    {
        return unavailableImageHandler;
    }

    /**
     * Poll the {@link Image}s under the subscription for having reached End of Stream.
     *
     * @param endOfStreamHandler callback for handling end of stream indication.
     * @return number of {@link Image} that have reached End of Stream.
     */
    public int pollEndOfStreams(final EndOfStreamHandler endOfStreamHandler)
    {
        int numEndOfStreams = 0;

        for (final Image image : images)
        {
            if (image.isEndOfStream())
            {
                numEndOfStreams++;
                endOfStreamHandler.onEndOfStream(image);
            }
        }

        return numEndOfStreams;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered within a session.
     * <p>
     * To assemble messages that span multiple fragments then use {@link FragmentAssembler}.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll operation across multiple {@link Image}s.
     * @return the number of fragments received
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        final Image[] images = this.images;
        final int length = images.length;
        int fragmentsRead = 0;

        int startingIndex = roundRobinIndex++;
        if (startingIndex >= length)
        {
            roundRobinIndex = startingIndex = 0;
        }

        for (int i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        for (int i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        return fragmentsRead;
    }

    /**
     * Poll in a controlled manner the {@link Image}s under the subscription for available message fragments.
     * Control is applied to fragments in the stream. If more fragments can be read on another stream
     * they will even if BREAK or ABORT is returned from the fragment handler.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered within a session.
     * <p>
     * To assemble messages that span multiple fragments then use {@link ControlledFragmentAssembler}.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll operation across multiple {@link Image}s.
     * @return the number of fragments received
     * @see ControlledFragmentHandler
     */
    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        final Image[] images = this.images;
        final int length = images.length;
        int fragmentsRead = 0;

        int startingIndex = roundRobinIndex++;
        if (startingIndex >= length)
        {
            roundRobinIndex = startingIndex = 0;
        }

        for (int i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].controlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        for (int i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].controlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        return fragmentsRead;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments in blocks.
     * <p>
     * This method is useful for operations like bulk archiving and messaging indexing.
     *
     * @param blockHandler     to receive a block of fragments from each {@link Image}.
     * @param blockLengthLimit for each {@link Image} polled.
     * @return the number of bytes consumed.
     */
    public long blockPoll(final BlockHandler blockHandler, final int blockLengthLimit)
    {
        long bytesConsumed = 0;
        for (final Image image : images)
        {
            bytesConsumed += image.blockPoll(blockHandler, blockLengthLimit);
        }

        return bytesConsumed;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments in blocks.
     * <p>
     * This method is useful for operations like bulk archiving a stream to file.
     *
     * @param rawBlockHandler  to receive a block of fragments from each {@link Image}.
     * @param blockLengthLimit for each {@link Image} polled.
     * @return the number of bytes consumed.
     */
    public long rawPoll(final RawBlockHandler rawBlockHandler, final int blockLengthLimit)
    {
        long bytesConsumed = 0;
        for (final Image image : images)
        {
            bytesConsumed += image.rawPoll(rawBlockHandler, blockLengthLimit);
        }

        return bytesConsumed;
    }

    /**
     * Is this subscription connected by having at least one publication {@link Image}.
     *
     * @return true if this subscription connected by having at least one publication {@link Image}.
     */
    public boolean isConnected()
    {
        return images.length > 0;
    }

    /**
     * Has the subscription currently no images connected to it?
     *
     * @return he subscription currently no images connected to it?
     */
    public boolean hasNoImages()
    {
        return images.length == 0;
    }

    /**
     * Count of images connected to this subscription.
     *
     * @return count of images connected to this subscription.
     */
    public int imageCount()
    {
        return images.length;
    }

    /**
     * Return the {@link Image} associated with the given sessionId.
     *
     * @param sessionId associated with the Image.
     * @return Image associated with the given sessionId or null if no Image exist.
     */
    public Image imageBySessionId(final int sessionId)
    {
        Image result = null;

        for (final Image image : images)
        {
            if (sessionId == image.sessionId())
            {
                result = image;
                break;
            }
        }

        return result;
    }

    /**
     * Get the image at the given index from the images array.
     *
     * @param index in the array
     * @return image at given index
     * @throws ArrayIndexOutOfBoundsException if the index is not valid.
     */
    public Image imageAtIndex(final int index)
    {
        return images[index];
    }

    /**
     * Get a {@link List} of active {@link Image}s that match this subscription.
     *
     * @return an unmodifiable {@link List} of active {@link Image}s that match this subscription.
     */
    public List<Image> images()
    {
        return Collections.unmodifiableList(Arrays.asList(images));
    }

    /**
     * Iterate over the {@link Image}s for this subscription.
     *
     * @param consumer to handle each {@link Image}.
     */
    public void forEachImage(final Consumer<Image> consumer)
    {
        for (final Image image : images)
        {
            consumer.accept(image);
        }
    }

    /**
     * Close the Subscription so that associated {@link Image}s can be released.
     * <p>
     * This method is idempotent.
     */
    public void close()
    {
        conductor.releaseSubscription(this);
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * Get the status of the media channel for this Subscription.
     * <p>
     * The status will be {@link io.aeron.status.ChannelEndpointStatus#ERRORED} if a socket exception occurs on setup
     * and {@link io.aeron.status.ChannelEndpointStatus#ACTIVE} if all is well.
     *
     * @return status for the channel as one of the constants from {@link ChannelEndpointStatus} with it being
     * {@link ChannelEndpointStatus#NO_ID_ALLOCATED} if the subscription is closed.
     * @see io.aeron.status.ChannelEndpointStatus
     */
    public long channelStatus()
    {
        if (isClosed)
        {
            return ChannelEndpointStatus.NO_ID_ALLOCATED;
        }

        return conductor.channelStatus(channelStatusId);
    }

    /**
     * Add a destination manually to a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to add
     */
    public void addDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Subscription is closed");
        }

        conductor.addRcvDestination(registrationId, endpointChannel);
    }

    /**
     * Remove a previously added destination from a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to remove
     */
    public void removeDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Subscription is closed");
        }

        conductor.removeRcvDestination(registrationId, endpointChannel);
    }

    void channelStatusId(final int id)
    {
        channelStatusId = id;
    }

    int channelStatusId()
    {
        return channelStatusId;
    }

    void internalClose()
    {
        isClosed = true;
        closeImages();
    }

    boolean containsImage(final long correlationId)
    {
        return imageIdSet.contains(correlationId);
    }

    void addImage(final Image image)
    {
        if (isClosed)
        {
            image.close();
            conductor.releaseImage(image);
        }
        else
        {
            if (imageIdSet.add(image.correlationId()))
            {
                images = ArrayUtil.add(images, image);
            }
        }
    }

    Image removeImage(final long correlationId)
    {
        final Image[] oldArray = images;
        Image removedImage = null;

        if (imageIdSet.remove(correlationId))
        {
            int i = 0;
            for (final Image image : oldArray)
            {
                if (image.correlationId() == correlationId)
                {
                    removedImage = image;
                    break;
                }

                i++;
            }

            images = ArrayUtil.remove(oldArray, i);
            removedImage.close();
            conductor.releaseImage(removedImage);
        }

        return removedImage;
    }

    private void closeImages()
    {
        final Image[] images = this.images;
        this.images = EMPTY_ARRAY;

        for (final Image image : images)
        {
            image.close();
        }

        imageIdSet.clear();

        for (final Image image : images)
        {
            conductor.releaseImage(image);

            try
            {
                if (null != unavailableImageHandler)
                {
                    unavailableImageHandler.onUnavailableImage(image);
                }
            }
            catch (final Throwable ex)
            {
                conductor.handleError(ex);
            }
        }
    }
}
