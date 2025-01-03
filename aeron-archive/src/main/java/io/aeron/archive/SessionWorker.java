/*
 * Copyright 2014-2025 Real Logic Limited.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archive;

import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CountedErrorHandler;

import java.util.ArrayList;

/**
 * This is a common workflow to {@link Session} handling in the archive. Hooks are provided for specialisation as
 * protected methods.
 *
 * @param <T> session type
 */
class SessionWorker<T extends Session> implements Agent
{
    private final ArrayList<T> sessions = new ArrayList<>();
    private final String roleName;
    protected final CountedErrorHandler errorHandler;
    private boolean isClosed = false;

    SessionWorker(final String roleName, final CountedErrorHandler errorHandler)
    {
        this.roleName = roleName;
        this.errorHandler = errorHandler;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return roleName;
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;

        final ArrayList<T> sessions = this.sessions;
        for (int lastIndex = sessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final T session = sessions.get(i);
            try
            {
                workCount += session.doWork();
            }
            catch (final Exception ex)
            {
                errorHandler.onError(ex);
            }

            if (session.isDone())
            {
                ArrayListUtil.fastUnorderedRemove(sessions, i, lastIndex--);
                closeSession(session);
            }
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public final void onClose()
    {
        if (isClosed)
        {
            return;
        }

        isClosed = true;

        try
        {
            preSessionsClose();
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }

        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            closeSession(sessions.get(i));
        }

        postSessionsClose();
    }

    /**
     * {@inheritDoc}
     */
    protected void abort()
    {
    }

    /**
     * {@inheritDoc}
     */
    protected void closeSession(final T session)
    {
        try
        {
            session.close();
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected void postSessionsClose()
    {
    }

    /**
     * {@inheritDoc}
     */
    protected void preSessionsClose()
    {
    }

    /**
     * {@inheritDoc}
     */
    protected void addSession(final T session)
    {
        sessions.add(session);
    }
}
