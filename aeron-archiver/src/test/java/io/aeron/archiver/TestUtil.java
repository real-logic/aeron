/*
 * Copyright 2014-2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver;

import org.junit.Assert;

import java.io.*;

class TestUtil
{
    static File makeTempDir() throws IOException
    {
        final File tempDirForTest = File.createTempFile("archiver.test", "tmp");
        // we really need a temp dir, not a file... delete and remake!
        Assert.assertTrue(tempDirForTest.delete());
        Assert.assertTrue(tempDirForTest.mkdir());

        return tempDirForTest;
    }
}
