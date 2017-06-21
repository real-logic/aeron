/*
 * Copyright 2015 Real Logic Ltd.
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

import io.aeron.CommonContext;
import io.aeron.driver.buffer.RawLog;
import io.aeron.AeronUri;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;

import static io.aeron.CommonContext.*;

class PublicationParams
{
    int termLength = 0;
    int mtuLength = 0;
    int initialTermId = 0;
    int termId = 0;
    int termOffset = 0;
    boolean isReplay = false;

    static int getTermBufferLength(final AeronUri aeronUri, final int defaultTermLength)
    {
        final String termLengthParam = aeronUri.get(CommonContext.TERM_LENGTH_PARAM_NAME);
        int termLength = defaultTermLength;
        if (null != termLengthParam)
        {
            termLength = Integer.parseInt(termLengthParam);
            LogBufferDescriptor.checkTermLength(termLength);
        }

        return termLength;
    }

    static int getMtuLength(final AeronUri aeronUri, final int defaultMtuLength)
    {
        int mtuLength = defaultMtuLength;
        final String mtu = aeronUri.get(CommonContext.MTU_LENGTH_PARAM_NAME);
        if (null != mtu)
        {
            mtuLength = Integer.parseInt(mtu);
            Configuration.validateMtuLength(mtuLength);
        }

        return mtuLength;
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
                params.mtuLength + " maxMessageLength=" + maxMessageLength);
        }
    }

    static void confirmMatch(
        final AeronUri uri, final PublicationParams params, final RawLog rawLog)
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
    }

    @SuppressWarnings("ConstantConditions")
    static PublicationParams getPublicationParams(
        final MediaDriver.Context context,
        final AeronUri aeronUri,
        final boolean isExclusive,
        final boolean isIpc)
    {
        final PublicationParams params = new PublicationParams();

        params.termLength = getTermBufferLength(
            aeronUri, isIpc ? context.ipcTermBufferLength() : context.publicationTermBufferLength());

        params.mtuLength = getMtuLength(aeronUri, isIpc ? context.ipcMtuLength() : context.mtuLength());

        if (isExclusive)
        {
            int count = 0;

            final String initialTermIdStr = aeronUri.get(INITIAL_TERM_ID_PARAM_NAME);
            count = initialTermIdStr != null ? count + 1 : count;

            final String termIdStr = aeronUri.get(TERM_ID_PARAM_NAME);
            count = termIdStr != null ? count + 1 : count;

            final String termOffsetStr = aeronUri.get(TERM_OFFSET_PARAM_NAME);
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

                params.isReplay = true;
            }
        }

        return params;
    }
}
