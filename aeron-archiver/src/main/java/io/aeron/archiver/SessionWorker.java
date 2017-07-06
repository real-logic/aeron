/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.Agent;

import java.util.ArrayList;

/**
 * This is a common workflow to {@link Session} handling in the archiver. Hooks are provided for specialization as
 * protected methods.
 *
 * @param <T> session type
 */
class SessionWorker<T extends Session> implements Agent
{
    private final ArrayList<T> sessions = new ArrayList<>();
    private final String roleName;
    private boolean isClosed = false;

    SessionWorker(final String roleName)
    {
        this.roleName = roleName;
    }

    public String roleName()
    {
        return roleName;
    }

    public final int doWork()
    {
        int workDone = preWork();

        final ArrayList<T> sessions = this.sessions;
        for (int lastIndex = sessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final T session = sessions.get(i);
            workDone += session.doWork();
            if (session.isDone())
            {
                closeSession(session);
                ArrayListUtil.fastUnorderedRemove(sessions, i, lastIndex);
                lastIndex--;
            }
        }

        return workDone;
    }

    public final void onClose()
    {
        if (isClosed)
        {
            return;
        }
        isClosed = true;

        preSessionsClose();
        sessions.forEach(this::closeSession);
        sessions.clear();
        postSessionsClose();
    }

    protected int preWork()
    {
        return 0;
    }

    protected void closeSession(final T session)
    {
        session.abort();
        session.close();
    }

    protected void postSessionsClose()
    {
    }

    protected void preSessionsClose()
    {
    }

    protected void postSessionAdd(final T session)
    {
    }

    boolean isClosed()
    {
        return isClosed;
    }

    protected void addSession(final T session)
    {
        sessions.add(session);
        postSessionAdd(session);
    }

    protected void abortSession(final T session)
    {
        session.abort();
    }
}
