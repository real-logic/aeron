package io.aeron.support;

import io.aeron.driver.MediaDriver;

import static org.agrona.Strings.isEmpty;

public interface TestMediaDriver extends AutoCloseable
{
    String AERON_TEST_SYSTEM_AERONMD_PATH = "aeron.test.system.aeronmd.path";

    static TestMediaDriver launch(MediaDriver.Context context)
    {
        return (isEmpty(System.getProperty(AERON_TEST_SYSTEM_AERONMD_PATH))) ?
            JavaTestMediaDriver.launch(context) :
            NativeTestMediaDriver.launch(context);
    }

    MediaDriver.Context context();
}
