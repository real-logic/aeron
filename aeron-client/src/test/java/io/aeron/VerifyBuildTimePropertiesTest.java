package io.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.JRE;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VerifyBuildTimePropertiesTest
{
    private static final String BUILD_JAVA_VERSION_ENV_VAR = "BUILD_JAVA_VERSION";

    @Test
    @EnabledIfEnvironmentVariable(named = BUILD_JAVA_VERSION_ENV_VAR, matches = "\\d+")
    void checkVersion()
    {
        final String version = System.getenv(BUILD_JAVA_VERSION_ENV_VAR);
        final String currentVersion = JRE.currentVersion().name();
        assertEquals(version, currentVersion.substring(currentVersion.indexOf('_') + 1));
    }
}
