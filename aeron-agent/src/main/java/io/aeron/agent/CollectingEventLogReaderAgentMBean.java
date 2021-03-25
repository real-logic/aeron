package io.aeron.agent;

/**
 * MBean interface for a logging agent that stores events in memory and allows them to be periodically written
 * out.  Useful within tests.
 */
public interface CollectingEventLogReaderAgentMBean
{
    /**
     * Enable or disable the collection of logs.
     *
     * @param isCollecting whether logs should be collected or not.
     */
    void setCollecting(boolean isCollecting);

    /**
     * Shows whether logs are being collected or not.
     *
     * @return true to indicate logs are being collected in memory.
     */
    boolean isCollecting();

    /**
     * Reset the internal positions within the collector discarding all previous logs.  Should be called periodically,
     * so to avoid consuming all available memory.
     */
    void reset();

    /**
     * Output the collected logs to file.
     *
     * @param filename file to write to.
     *
     */
    void writeToFile(final String filename);
}
