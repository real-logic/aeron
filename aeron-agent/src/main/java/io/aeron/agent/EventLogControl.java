package io.aeron.agent;

public class EventLogControl implements EventLogControlMXBean
{
    @Override
    public boolean enableLogging()
    {
        return EventLogAgent.enableLogging();
    }

    @Override
    public boolean disableLogging()
    {
        return EventLogAgent.disableLogging();
    }

    @Override
    public boolean isLogging()
    {
        return EventLogAgent.isLogging();
    }
}
