package io.aeron.support;

import io.aeron.driver.MediaDriver;
import org.junit.Assume;

import static org.agrona.Strings.isEmpty;

public interface TestMediaDriver extends AutoCloseable
{
    String AERON_TEST_SYSTEM_AERONMD_PATH = "aeron.test.system.aeronmd.path";

    static boolean shouldRunCMediaDriver()
    {
        return !isEmpty(System.getProperty(AERON_TEST_SYSTEM_AERONMD_PATH));
    }

    static void notSupportedOnCMediaDriverYet(String reason)
    {
        Assume.assumeFalse("Functionality not support by C Media Driver: " + reason, shouldRunCMediaDriver());
    }

    static TestMediaDriver launch(MediaDriver.Context context)
    {
        return shouldRunCMediaDriver() ?
            CTestMediaDriver.launch(context, null) : JavaTestMediaDriver.launch(context);
    }

    static TestMediaDriver launch(MediaDriver.Context context, DriverOutputConsumer driverOutputConsumer)
    {
        return shouldRunCMediaDriver() ?
            CTestMediaDriver.launch(context, driverOutputConsumer) : JavaTestMediaDriver.launch(context);
    }

    MediaDriver.Context context();

    String aeronDirectoryName();

    @Override
    void close();
}
