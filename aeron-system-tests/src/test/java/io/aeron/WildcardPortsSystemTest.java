package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.mock;

public class WildcardPortsSystemTest
{
    private static final int STREAM_ID = 2002;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[16]);
    private final FragmentHandler fragmentHandler = mock(FragmentHandler.class);

    private TestMediaDriver driver;
    private Aeron client;

    @BeforeEach
    void launch()
    {
        buffer.putInt(0, 1);

        final MediaDriver.Context context = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        driver = TestMediaDriver.launch(context);
        client = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(client, driver);
    }

    @Test
    @Timeout(5)
    void shouldSubscribeToWildcardPorts()
    {
        final String wildCardUri1 = "aeron:udp?endpoint=127.0.0.1:0|tags=1002";
        final String wildCardUri2 = "aeron:udp?endpoint=127.0.0.1:0|tags=1003";
        final String tagged1 = "aeron:udp?tags=1002";

        try (Subscription sub1 = client.addSubscription(wildCardUri1, STREAM_ID);
            Subscription sub2 = client.addSubscription(wildCardUri2, STREAM_ID);
            Subscription sub3 = client.addSubscription(tagged1, STREAM_ID + 1))
        {
            List<String> bindAddressAndPort1;
            while ((bindAddressAndPort1 = sub1.bindAddressAndPort()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub2");
            }
            List<String> bindAddressAndPort2;
            while ((bindAddressAndPort2 = sub2.bindAddressAndPort()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub3");
            }

            assertNotEquals(bindAddressAndPort1, bindAddressAndPort2);

            List<String> bindAddressAndPort3;
            while ((bindAddressAndPort3 = sub3.bindAddressAndPort()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub4");
            }

            assertEquals(bindAddressAndPort3, bindAddressAndPort1);

            final String pub2Uri = new ChannelUriStringBuilder()
                .media("udp").endpoint(bindAddressAndPort1.get(0))
                .build();

            try (Publication pub = client.addPublication(pub2Uri, STREAM_ID))
            {
                while (pub.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub2");
                }

                while (sub1.poll(fragmentHandler, 1) < 0)
                {
                    Tests.yieldingWait("Failed to receive from sub2");
                }
            }
        }
    }

    @Test
    @Timeout(5)
    void shouldSubscribeToWildcardPortsUsingIPv6()
    {
        assumeFalse(Boolean.getBoolean("java.net.preferIPv4Stack"));

        final String wildCardUri1 = "aeron:udp?endpoint=[::1]:0|tags=1001";
        final String tagged2 = "aeron:udp?tags=1001";

        try (Subscription sub1 = client.addSubscription(wildCardUri1, STREAM_ID);
            Subscription sub4 = client.addSubscription(tagged2, STREAM_ID + 1))
        {
            List<String> bindAddressAndPort1;
            while ((bindAddressAndPort1 = sub1.bindAddressAndPort()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub1");
            }

            List<String> bindAddressAndPort4;
            while ((bindAddressAndPort4 = sub4.bindAddressAndPort()).isEmpty())
            {
                Tests.yieldingWait("No bind address/port for sub4");
            }

            assertEquals(bindAddressAndPort4, bindAddressAndPort1);

            final String pub1Uri = new ChannelUriStringBuilder()
                .media("udp").endpoint(bindAddressAndPort1.get(0))
                .build();

            try (Publication pub1 = client.addPublication(pub1Uri, STREAM_ID))
            {
                while (pub1.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub1");
                }

                while (sub1.poll(fragmentHandler, 1) < 0)
                {
                    Tests.yieldingWait("Failed to receive from sub1");
                }
            }
        }
    }

    @Test
    @Timeout(5)
    void shouldBindMultipleWildcardsToMultiDestinationSubscription()
    {
        final String wildCardUri2 = "aeron:udp?endpoint=127.0.0.1:0";
        final String wildCardUri3 = "aeron:udp?endpoint=127.0.0.1:0";

        try (Subscription mdsSub = client.addSubscription("aeron:udp?control-mode=manual", STREAM_ID))
        {
            mdsSub.addDestination(wildCardUri2);
            mdsSub.addDestination(wildCardUri3);

            List<String> bindAddressAndPorts;
            while (2 > (bindAddressAndPorts = mdsSub.bindAddressAndPort()).size())
            {
                Tests.yieldingWait("Unable to get bind address/ports for mds subscription");
            }

            final String pub1Uri = new ChannelUriStringBuilder()
                .media("udp").endpoint(bindAddressAndPorts.get(0))
                .build();

            final String pub2Uri = new ChannelUriStringBuilder()
                .media("udp").endpoint(bindAddressAndPorts.get(1))
                .build();

            try (Publication pub1 = client.addPublication(pub1Uri, STREAM_ID);
                Publication pub2 = client.addPublication(pub2Uri, STREAM_ID))
            {
                while (pub1.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub1");
                }

                while (pub2.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub2");
                }

                long totalReceived = 0;
                while ((totalReceived += mdsSub.poll(fragmentHandler, 10)) < 2)
                {
                    Tests.yieldingWait("Failed to receive from both publications");
                }
            }
        }
    }
}
