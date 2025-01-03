/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PublicationGroupTest
{
    private final String channelTemplate = "aeron:udp?term-length=64k";
    private final int streamId = 10000;
    private final String[] endpoints = { "localhost:1001", "localhost:1002", "localhost:1003" };
    private final PublicationGroup<ExclusivePublication> publicationGroup = new PublicationGroup<>(
        endpoints, channelTemplate, streamId, Aeron::addExclusivePublication);
    private final Aeron mockAeron = mock(Aeron.class);
    private final List<ExclusivePublication> publications = Arrays.asList(
        mock(ExclusivePublication.class, "pub_localhost:1001"),
        mock(ExclusivePublication.class, "pub_localhost:1002"),
        mock(ExclusivePublication.class, "pub_localhost:1003"));

    @BeforeEach
    void setUp()
    {
        when(mockAeron.addExclusivePublication(matches(".*localhost:1001.*"), anyInt()))
            .thenReturn(publications.get(0));
        when(mockAeron.addExclusivePublication(matches(".*localhost:1002.*"), anyInt()))
            .thenReturn(publications.get(1));
        when(mockAeron.addExclusivePublication(matches(".*localhost:1003.*"), anyInt()))
            .thenReturn(publications.get(2));
    }

    @Test
    void shouldUseAllPublicationsInListWhenGettingNextPublication()
    {
        publicationGroup.next(mockAeron);
        publicationGroup.next(mockAeron);
        publicationGroup.next(mockAeron);

        verify(mockAeron).addExclusivePublication("aeron:udp?endpoint=localhost:1001|term-length=64k", streamId);
        verify(mockAeron).addExclusivePublication("aeron:udp?endpoint=localhost:1002|term-length=64k", streamId);
        verify(mockAeron).addExclusivePublication("aeron:udp?endpoint=localhost:1003|term-length=64k", streamId);
    }

    @Test
    void shouldNextPublicationAndReturnSameWithCurrent()
    {
        final ExclusivePublication pubNext = publicationGroup.next(mockAeron);
        final ExclusivePublication pubCurrent1 = publicationGroup.current();
        final ExclusivePublication pubCurrent2 = publicationGroup.current();

        assertSame(pubNext, pubCurrent1);
        assertSame(pubNext, pubCurrent2);

        verify(mockAeron, times(1)).addExclusivePublication(anyString(), anyInt());
    }

    @Test
    void shouldResultNullWhenCurrentCalledWithoutNext()
    {
        assertNull(publicationGroup.current());
    }

    @Test
    void shouldCloseWhenCurrentIsNotNullWhenNextIsCalled()
    {
        assertNull(publicationGroup.current());
        final ExclusivePublication current = publicationGroup.next(mockAeron);

        assertNotNull(publicationGroup.next(mockAeron));
        verify(current).close();
    }

    @Test
    void shouldExcludeCurrentPublication()
    {
        final ExclusivePublication toExclude = publicationGroup.next(mockAeron);
        publicationGroup.closeAndExcludeCurrent();

        for (int i = 0; i < 100; i++)
        {
            assertNotSame(toExclude, publicationGroup.next(mockAeron));
        }
    }

    @Test
    void shouldClearExclusion()
    {
        final ExclusivePublication toExclude = publicationGroup.next(mockAeron);
        publicationGroup.closeAndExcludeCurrent();
        assertNotSame(toExclude, publicationGroup.next(mockAeron));

        for (int i = 0; i < publications.size(); i++)
        {
            assertNotSame(toExclude, publicationGroup.next(mockAeron));
        }

        publicationGroup.clearExclusion();

        boolean found = false;
        for (int i = 0; i < publications.size(); i++)
        {
            if (toExclude == publicationGroup.next(mockAeron))
            {
                found = true;
            }
        }

        assertTrue(found);
    }

    @Test
    void shouldClosePublicationOnClose()
    {
        final ExclusivePublication pub = publicationGroup.next(mockAeron);
        publicationGroup.close();
        verify(pub).close();
    }

    @Test
    void shouldEventuallyGetADifferentOrderAfterShuffle()
    {
        final String[] originalOrder = Arrays.copyOf(endpoints, endpoints.length);
        int differenceCount = 0;
        for (int i = 0; i < 100; i++)
        {
            publicationGroup.shuffle();
            differenceCount += !Arrays.equals(originalOrder, endpoints) ? 1 : 0;
        }

        assertNotEquals(0, differenceCount);
    }
}