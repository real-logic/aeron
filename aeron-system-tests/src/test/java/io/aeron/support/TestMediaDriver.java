package io.aeron.support;

import io.aeron.driver.MediaDriver;

public interface TestMediaDriver extends AutoCloseable
{
    String AERON_TEST_SYSTEM_AERONMD_PATH = "aeron.test.system.aeronmd.path";

    static TestMediaDriver launch(MediaDriver.Context context)
    {
        return (null != System.getProperty(AERON_TEST_SYSTEM_AERONMD_PATH)) ?
            NativeTestMediaDriver.launch(context) :
            JavaTestMediaDriver.launch(context);
    }
}
