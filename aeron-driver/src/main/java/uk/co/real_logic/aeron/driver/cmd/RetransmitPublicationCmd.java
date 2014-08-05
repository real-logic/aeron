/*
 * Copyright 2014 Real Logic Ltd.
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

package uk.co.real_logic.aeron.driver.cmd;

import uk.co.real_logic.aeron.driver.DriverPublication;

public class RetransmitPublicationCmd
{
    private final DriverPublication driverPublication;
    private final int termId;
    private final int termOffset;

    public RetransmitPublicationCmd(final DriverPublication driverPublication, final int termId, final int termOffset)
    {
        this.driverPublication = driverPublication;
        this.termId = termId;
        this.termOffset = termOffset;
    }

    public DriverPublication driverPublication()
    {
        return driverPublication;
    }

    public int termId()
    {
        return termId;
    }

    public int termOffset()
    {
        return termOffset;
    }
}
