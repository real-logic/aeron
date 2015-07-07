/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.logbuffer.BlockHandler;
import uk.co.real_logic.aeron.logbuffer.FileBlockHandler;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;

/**
 * Aeron Subscriber API for receiving {@link Image}s of messages from publishers on a given channel and streamId pair.
 * Subscribers are created via an {@link Aeron} object, and received messages are delivered
 * to the {@link FragmentHandler}.
 * <p>
 * By default fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, whether or not they were fragmented, then the Subscriber
 * should be created with a {@link FragmentAssembler} or a custom implementation.
 * <p>
 * It is an applications responsibility to {@link #poll} the Subscriber for new messages.
 * <p>
 * Subscriptions are not threadsafe and should not be shared between subscribers.
 *
 * @see FragmentAssembler
 */
public class Subscription implements AutoCloseable
{
    private static final Image[] EMPTY_ARRAY = new Image[0];

    private final long registrationId;
    private final int streamId;
    private int roundRobinIndex = 0;
    private volatile boolean isClosed = false;

    private volatile Image[] images = EMPTY_ARRAY;
    private final ClientConductor clientConductor;
    private final String channel;

    Subscription(final ClientConductor conductor, final String channel, final int streamId, final long registrationId)
    {
        this.clientConductor = conductor;
        this.channel = channel;
        this.streamId = streamId;
        this.registrationId = registrationId;
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
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for a single poll operation.
     * @return the number of fragments received
     * @throws IllegalStateException if the subscription is closed.
     * @see FragmentAssembler
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        ensureOpen();

        final Image[] images = this.images;
        final int length = images.length;
        int fragmentsRead = 0;

        if (length > 0)
        {
            int startingIndex = roundRobinIndex++;
            if (startingIndex >= length)
            {
                roundRobinIndex = startingIndex = 0;
            }

            int i = startingIndex;

            do
            {
                fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit);

                if (++i == length)
                {
                    i = 0;
                }
            }
            while (fragmentsRead < fragmentLimit && i != startingIndex);
        }

        return fragmentsRead;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments in blocks.
     *
     * @param blockHandler     to receive a block of fragments from each {@link Image}.
     * @param blockLengthLimit for each individual block.
     * @return the number of bytes consumed.
     * @throws IllegalStateException if the subscription is closed.
     */
    public long blockPoll(final BlockHandler blockHandler, final int blockLengthLimit)
    {
        ensureOpen();

        long bytesConsumed = 0;
        final Image[] images = this.images;
        for (final Image image : images)
        {
            bytesConsumed += image.blockPoll(blockHandler, blockLengthLimit);
        }

        return bytesConsumed;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments in blocks.
     *
     * @param fileBlockHandler to receive a block of fragments from each {@link Image}.
     * @param blockLengthLimit for each individual block.
     * @return the number of bytes consumed.
     * @throws IllegalStateException if the subscription is closed.
     */
    public long filePoll(final FileBlockHandler fileBlockHandler, final int blockLengthLimit)
    {
        ensureOpen();

        long bytesConsumed = 0;
        final Image[] images = this.images;
        for (final Image image : images)
        {
            bytesConsumed += image.filePoll(fileBlockHandler, blockLengthLimit);
        }

        return bytesConsumed;
    }

    /**
     * Close the Subscription so that associated {@link Image} can be released.
     * <p>
     * This method is idempotent.
     */
    public void close()
    {
        synchronized (clientConductor)
        {
            if (!isClosed)
            {
                isClosed = true;

                clientConductor.releaseSubscription(this);

                final Image[] images = this.images;
                for (final Image image : images)
                {
                    clientConductor.lingerResource(image.managedResource());
                }
                this.images = EMPTY_ARRAY;
            }
        }
    }

    long registrationId()
    {
        return registrationId;
    }

    void addImage(final Image image)
    {
        final Image[] oldArray = images;
        final int oldLength = oldArray.length;
        final Image[] newArray = new Image[oldLength + 1];

        System.arraycopy(oldArray, 0, newArray, 0, oldLength);
        newArray[oldLength] = image;

        images = newArray;
    }

    Image removeImage(final long correlationId)
    {
        final Image[] oldArray = images;
        final int oldLength = oldArray.length;
        Image removedImage = null;
        int index = -1;

        for (int i = 0; i < oldLength; i++)
        {
            if (oldArray[i].correlationId() == correlationId)
            {
                index = i;
                removedImage = oldArray[i];
            }
        }

        if (null != removedImage)
        {
            final int newLength = oldLength - 1;
            final Image[] newArray = new Image[newLength];
            System.arraycopy(oldArray, 0, newArray, 0, index);
            System.arraycopy(oldArray, index + 1, newArray, index, newLength - index);
            images = newArray;

            clientConductor.lingerResource(removedImage.managedResource());
        }

        return removedImage;
    }

    boolean hasImage(final int sessionId)
    {
        boolean isConnected = false;

        final Image[] images = this.images;
        for (final Image image : images)
        {
            if (sessionId == image.sessionId())
            {
                isConnected = true;
                break;
            }
        }

        return isConnected;
    }

    boolean hasNoImages()
    {
        return images.length == 0;
    }

    private void ensureOpen()
    {
        if (isClosed)
        {
            throw new IllegalStateException(String.format(
                "Subscription is closed: channel=%s streamId=%d registrationId=%d", channel, streamId, registrationId));
        }
    }
}
