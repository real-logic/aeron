package io.aeron;

import org.junit.jupiter.api.Test;

class PrintEnvInfo
{
    @Test
    void test()
    {
        System.out.println("=========================");
        System.out.println("[PrintEnvInfo] System properties:");
        System.out.println("=========================");
        System.getProperties().entrySet().stream()
            .filter(e -> ((String)e.getKey()).contains("java."))
            .forEach(e -> System.out.println("- " + e.getKey() + ": " + e.getValue()));

        System.out.println("\n=========================");
        System.out.println("[PrintEnvInfo] ENV variables:");
        System.out.println("=========================");
        for (final String env : new String[]{ "JAVA_HOME", "BUILD_JAVA_HOME", "BUILD_JAVA_VERSION", "PATH" })
        {
            System.out.println("- " + env + ": " + System.getenv(env));
        }
    }
}
