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

import io.aeron.driver.media.UdpChannel;
import org.agrona.LangUtil;

public class DefaultUnicastFlowControlSupplier implements FlowControlSupplier
{
    public FlowControl newInstance(final UdpChannel udpChannel, final int streamId, final long registrationId)
    {
        FlowControl flowControl = null;
        try
        {
            flowControl = (FlowControl)Class.forName(Configuration.UNICAST_FLOW_CONTROL_STRATEGY)
                .getConstructor()
                .newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return flowControl;
    }
}
