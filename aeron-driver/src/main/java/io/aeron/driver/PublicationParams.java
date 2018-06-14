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
package io.aeron.driver;

import io.aeron.driver.buffer.RawLog;
import io.aeron.ChannelUri;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.SystemUtil;

import static io.aeron.ChannelUri.INVALID_TAG;
import static io.aeron.CommonContext.*;

final class PublicationParams
{
    long lingerTimeoutNs;
    long tag = ChannelUri.INVALID_TAG;
    int termLength;
    int mtuLength;
    int initialTermId = 0;
    int termId = 0;
    int termOffset = 0;
    int sessionId = 0;
    boolean isReplay = false;
    boolean hasSessionId = false;
    boolean isSessionIdTagged = false;

    private PublicationParams(final MediaDriver.Context context, final boolean isIpc)
    {
        termLength = isIpc ? context.ipcTermBufferLength() : context.publicationTermBufferLength();
        mtuLength = isIpc ? context.ipcMtuLength() : context.mtuLength();
        lingerTimeoutNs = context.publicationLingerTimeoutNs();
    }

    private void getTag(final ChannelUri channelUri, final DriverConductor driverConductor)
    {
        final String tagParam = channelUri.entityTag();
        if (null != tagParam)
        {
            final long tag = Long.parseLong(tagParam);
            validateTag(tag, driverConductor);
            this.tag = tag;
        }
    }

    private void getTermBufferLength(final ChannelUri channelUri)
    {
        final String termLengthParam = channelUri.get(TERM_LENGTH_PARAM_NAME);
        if (null != termLengthParam)
        {
            final int termLength = (int)SystemUtil.parseSize(TERM_LENGTH_PARAM_NAME, termLengthParam);
            LogBufferDescriptor.checkTermLength(termLength);
            validateTermLength(this, termLength);
            this.termLength = termLength;
        }
    }

    private void getMtuLength(final ChannelUri channelUri)
    {
        final String mtuParam = channelUri.get(MTU_LENGTH_PARAM_NAME);
        if (null != mtuParam)
        {
            final int mtuLength = (int)SystemUtil.parseSize(MTU_LENGTH_PARAM_NAME, mtuParam);
            Configuration.validateMtuLength(mtuLength);
            validateMtuLength(this, mtuLength);
            this.mtuLength = mtuLength;
        }
    }

    private void getLingerTimeoutNs(final ChannelUri channelUri)
    {
        final String lingerParam = channelUri.get(LINGER_PARAM_NAME);
        if (null != lingerParam)
        {
            lingerTimeoutNs = SystemUtil.parseDuration(LINGER_PARAM_NAME, lingerParam);
            Configuration.validatePublicationLingerTimeoutNs(lingerTimeoutNs, lingerTimeoutNs);
        }
    }

    private void getSessionId(final ChannelUri channelUri, final DriverConductor driverConductor)
    {
        final String sessionIdStr = channelUri.get(SESSION_ID_PARAM_NAME);
        if (null != sessionIdStr)
        {
            isSessionIdTagged = ChannelUri.isTagged(sessionIdStr);
            if (isSessionIdTagged)
            {
                final NetworkPublication publication = driverConductor.findNetworkPublicationByTag(
                    ChannelUri.getTag(sessionIdStr));

                if (null == publication)
                {
                    throw new IllegalArgumentException(
                        SESSION_ID_PARAM_NAME + "=" + sessionIdStr + " must reference a network publication");
                }

                sessionId = publication.sessionId();
                mtuLength = publication.mtuLength();
                termLength = publication.termBufferLength();
            }
            else
            {
                sessionId = Integer.parseInt(sessionIdStr);
            }

            hasSessionId = true;
        }
    }

    static void validateMtuForMaxMessage(final PublicationParams params, final boolean isExclusive)
    {
        final int termLength = params.termLength;
        final int maxMessageLength = isExclusive ?
            FrameDescriptor.computeExclusiveMaxMessageLength(termLength) :
            FrameDescriptor.computeMaxMessageLength(termLength);

        if (params.mtuLength > maxMessageLength)
        {
            throw new IllegalStateException("MTU greater than max message length for term length: mtu=" +
                params.mtuLength + " maxMessageLength=" + maxMessageLength + " termLength=" + termLength);
        }
    }

    static void validateTermLength(final PublicationParams params, final int explicitTermLength)
    {
        if (params.isSessionIdTagged && explicitTermLength != params.termLength)
        {
            throw new IllegalArgumentException(
                TERM_LENGTH_PARAM_NAME + "=" + explicitTermLength + " does not match session-id tag value");
        }
    }

    static void validateMtuLength(final PublicationParams params, final int explicitMtuLength)
    {
        if (params.isSessionIdTagged && explicitMtuLength != params.mtuLength)
        {
            throw new IllegalArgumentException(
                MTU_LENGTH_PARAM_NAME + "=" + explicitMtuLength + " does not match session-id tag value");
        }
    }

    static void confirmMatch(
        final ChannelUri uri, final PublicationParams params, final RawLog rawLog, final int existingSessionId)
    {
        final int mtuLength = LogBufferDescriptor.mtuLength(rawLog.metaData());
        if (uri.containsKey(MTU_LENGTH_PARAM_NAME) && mtuLength != params.mtuLength)
        {
            throw new IllegalStateException("Existing publication has different MTU length: existing=" +
                mtuLength + " requested=" + params.mtuLength);
        }

        if (uri.containsKey(TERM_LENGTH_PARAM_NAME) && rawLog.termLength() != params.termLength)
        {
            throw new IllegalStateException("Existing publication has different term length: existing=" +
                rawLog.termLength() + " requested=" + params.termLength);
        }

        if (uri.containsKey(SESSION_ID_PARAM_NAME) && params.sessionId != existingSessionId)
        {
            throw new IllegalStateException("Existing publication has different session id: existing=" +
                existingSessionId + " requested=" + params.sessionId);
        }
    }

    private static void validateTag(final long tag, final DriverConductor driverConductor)
    {
        if (INVALID_TAG == tag)
        {
            throw new IllegalArgumentException(INVALID_TAG + " tag is reserved");
        }

        if (null != driverConductor.findNetworkPublicationByTag(tag) ||
            null != driverConductor.findIpcPublicationByTag(tag))
        {
            throw new IllegalArgumentException(tag + " tag already in use");
        }
    }

    @SuppressWarnings("ConstantConditions")
    static PublicationParams getPublicationParams(
        final MediaDriver.Context context,
        final ChannelUri channelUri,
        final DriverConductor driverConductor,
        final boolean isExclusive,
        final boolean isIpc)
    {
        final PublicationParams params = new PublicationParams(context, isIpc);

        params.getTag(channelUri, driverConductor);
        params.getSessionId(channelUri, driverConductor);
        params.getTermBufferLength(channelUri);
        params.getMtuLength(channelUri);
        params.getLingerTimeoutNs(channelUri);

        if (isExclusive)
        {
            int count = 0;

            final String initialTermIdStr = channelUri.get(INITIAL_TERM_ID_PARAM_NAME);
            count = initialTermIdStr != null ? count + 1 : count;

            final String termIdStr = channelUri.get(TERM_ID_PARAM_NAME);
            count = termIdStr != null ? count + 1 : count;

            final String termOffsetStr = channelUri.get(TERM_OFFSET_PARAM_NAME);
            count = termOffsetStr != null ? count + 1 : count;

            if (count > 0)
            {
                if (count < 3)
                {
                    throw new IllegalArgumentException("Params must be used as a complete set: " +
                        INITIAL_TERM_ID_PARAM_NAME + " " + TERM_ID_PARAM_NAME + " " + TERM_OFFSET_PARAM_NAME);
                }

                params.initialTermId = Integer.parseInt(initialTermIdStr);
                params.termId = Integer.parseInt(termIdStr);
                params.termOffset = Integer.parseInt(termOffsetStr);

                if (params.termOffset > params.termLength)
                {
                    throw new IllegalArgumentException(
                        TERM_OFFSET_PARAM_NAME + "=" + params.termOffset + " > " +
                        TERM_LENGTH_PARAM_NAME + "=" + params.termLength);
                }

                if (params.termOffset < 0)
                {
                    throw new IllegalArgumentException(
                        TERM_OFFSET_PARAM_NAME + "=" + params.termOffset + " must be greater than zero");
                }

                if ((params.termOffset & (FrameDescriptor.FRAME_ALIGNMENT - 1)) != 0)
                {
                    throw new IllegalArgumentException(
                        TERM_OFFSET_PARAM_NAME + "=" + params.termOffset + " must be a multiple of FRAME_ALIGNMENT");
                }

                params.isReplay = true;
            }
        }

        return params;
    }
}
