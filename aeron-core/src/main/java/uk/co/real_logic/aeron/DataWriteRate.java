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
package uk.co.real_logic.aeron;

/**
 * Record the data write rate as part of the buffer exhaustion process.
 */
public class DataWriteRate
{

    private long nanoTime;
    private long amountInBytes;

    public long nanoTime()
    {
        return nanoTime;
    }

    public void nanoTime(long nanoTime)
    {
        this.nanoTime = nanoTime;
    }

    public long amountInBytes()
    {
        return amountInBytes;
    }

    public void amountInBytes(final long amountInBytes)
    {
        this.amountInBytes = amountInBytes;
    }

}
