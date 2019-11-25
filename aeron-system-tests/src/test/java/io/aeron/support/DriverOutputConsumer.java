package io.aeron.support;

import java.io.File;

public interface DriverOutputConsumer
{
    void outputFiles(String aeronDirectoryName, File stdoutFile, File stderrFile);
}
