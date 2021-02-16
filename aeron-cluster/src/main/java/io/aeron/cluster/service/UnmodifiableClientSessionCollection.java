/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.cluster.service;

import org.agrona.collections.Long2ObjectHashMap;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

class UnmodifiableClientSessionCollection implements Collection<ClientSession>
{
    private final Long2ObjectHashMap<ClientSession>.ValueCollection collection;
    private final ClientSessionIterator iteratorWrapper = new ClientSessionIterator();

    UnmodifiableClientSessionCollection(final Long2ObjectHashMap<ClientSession>.ValueCollection collection)
    {
        this.collection = collection;
    }

    public int size()
    {
        return collection.size();
    }

    public boolean isEmpty()
    {
        return collection.isEmpty();
    }

    public boolean contains(final Object o)
    {
        return collection.contains(o);
    }

    public Object[] toArray()
    {
        return collection.toArray();
    }

    public <T> T[] toArray(final T[] a)
    {
        return collection.toArray(a);
    }

    public String toString()
    {
        return collection.toString();
    }

    public ClientSessionIterator iterator()
    {
        return iteratorWrapper.iterator(collection.iterator());
    }

    public boolean add(final ClientSession e)
    {
        throw new UnsupportedOperationException();
    }

    public boolean remove(final Object o)
    {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(final Collection<?> collection)
    {
        return this.collection.containsAll(collection);
    }

    public boolean addAll(final Collection<? extends ClientSession> collection)
    {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(final Collection<?> collection)
    {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(final Collection<?> collection)
    {
        throw new UnsupportedOperationException();
    }

    public void clear()
    {
        throw new UnsupportedOperationException();
    }

    public void forEach(final Consumer<? super ClientSession> action)
    {
        collection.forEach(action);
    }

    public boolean removeIf(final Predicate<? super ClientSession> filter)
    {
        throw new UnsupportedOperationException();
    }

    public Spliterator<ClientSession> spliterator()
    {
        return collection.spliterator();
    }

    public Stream<ClientSession> stream()
    {
        return collection.stream();
    }

    public Stream<ClientSession> parallelStream()
    {
        return collection.parallelStream();
    }

    static class ClientSessionIterator implements Iterator<ClientSession>
    {
        private Iterator<? extends ClientSession> iterator;

        ClientSessionIterator iterator(final Iterator<? extends ClientSession> iterator)
        {
            this.iterator = iterator;
            return this;
        }

        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        public ClientSession next()
        {
            return iterator.next();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        public void forEachRemaining(final Consumer<? super ClientSession> action)
        {
            iterator.forEachRemaining(action);
        }
    }
}
