package io.aeron.agent;

import javax.management.MXBean;

@MXBean
public interface EventLogControlMXBean
{
    boolean enableLogging();

    boolean disableLogging();

    boolean isLogging();
}
