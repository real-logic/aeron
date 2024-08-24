/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive.checksum;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.condition.JRE.JAVA_9;

class ChecksumsTest
{
    @Test
    void crc32()
    {
        assertSame(Crc32.INSTANCE, Checksums.crc32());
    }

    @EnabledForJreRange(min = JAVA_9)
    @Test
    void crc32c()
    {
        assertSame(Crc32c.INSTANCE, Checksums.crc32c());
    }

    @Test
    void newInstanceReturnsSameInstanceOfCrc32()
    {
        assertSame(Crc32.INSTANCE, Checksums.newInstance(Crc32.class.getName()));
    }

    @EnabledForJreRange(min = JAVA_9)
    @Test
    void newInstanceReturnsSameInstanceOfCrc32c()
    {
        assertSame(Crc32c.INSTANCE, Checksums.newInstance(Crc32c.class.getName()));
    }

    @Test
    void newInstanceThrowsNullPointerExceptionIfClassNameIsNull()
    {
        assertThrows(NullPointerException.class, () -> Checksums.newInstance(null));
    }

    @Test
    void newInstanceThrowsIllegalArgumentExceptionIfClassIsNotFound()
    {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class, () -> Checksums.newInstance("a.b.c.MissingClass"));
        assertEquals(ClassNotFoundException.class, exception.getCause().getClass());
    }

    @Test
    void newInstanceThrowsIllegalArgumentExceptionIfInstanceCannotBeCreated()
    {
        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class, () -> Checksums.newInstance(TimeUnit.class.getName()));
        assertEquals(NoSuchMethodException.class, exception.getCause().getClass());
    }

    @Test
    void newInstanceThrowsClassCastExceptionIfCreatedInstanceDoesNotImplementChecksumInterface()
    {
        assertThrows(ClassCastException.class, () -> Checksums.newInstance(Object.class.getName()));
    }

    @Test
    void newInstanceCreatesANewInstanceOfTheSpecifiedClass()
    {
        final Checksum instance1 = Checksums.newInstance(TestChecksum.class.getName());
        final Checksum instance2 = Checksums.newInstance(TestChecksum.class.getName());
        assertInstanceOf(TestChecksum.class, instance1);
        assertInstanceOf(TestChecksum.class, instance2);
        assertNotSame(instance1, instance2);
    }

    static class TestChecksum implements Checksum
    {
        public int compute(final long address, final int offset, final int length)
        {
            return 0;
        }
    }
}