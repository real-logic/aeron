package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
        final String wildCardUri1 = "aeron:udp?endpoint=[::1]:0|tags=1001";
        final String wildCardUri2 = "aeron:udp?endpoint=127.0.0.1:0|tags=1002";
        final String wildCardUri3 = "aeron:udp?endpoint=127.0.0.1:0|tags=1003";
        final String tagged2 = "aeron:udp?tags=1002";

        try (Subscription sub1 = client.addSubscription(wildCardUri1, STREAM_ID);
            Subscription sub2 = client.addSubscription(wildCardUri2, STREAM_ID);
            Subscription sub3 = client.addSubscription(wildCardUri3, STREAM_ID);
            Subscription sub4 = client.addSubscription(tagged2, STREAM_ID + 1))
        {
            String bindAddressAndPort1;
            while (null == (bindAddressAndPort1 = sub1.bindAddressAndPort().get(0)))
            {
                Tests.yieldingWait("No bind address port for sub1");
            }
            String bindAddressAndPort2;
            while (null == (bindAddressAndPort2 = sub2.bindAddressAndPort().get(0)))
            {
                Tests.yieldingWait("No bind address port for sub2");
            }
            String bindAddressAndPort3;
            while (null == (bindAddressAndPort3 = sub3.bindAddressAndPort().get(0)))
            {
                Tests.yieldingWait("No bind address port for sub3");
            }

            assertNotEquals(bindAddressAndPort1, bindAddressAndPort2);
            assertNotEquals(bindAddressAndPort1, bindAddressAndPort3);
            assertNotEquals(bindAddressAndPort2, bindAddressAndPort3);

            String bindAddressAndPort4;
            while (null == (bindAddressAndPort4 = sub4.bindAddressAndPort().get(0)))
            {
                Tests.yieldingWait("No bind address port for sub4");
            }

            assertEquals(bindAddressAndPort4, bindAddressAndPort2);

            final String pub1Uri = new ChannelUriStringBuilder().media("udp").endpoint(bindAddressAndPort1).build();
            final String pub2Uri = new ChannelUriStringBuilder().media("udp").endpoint(bindAddressAndPort2).build();


            try (Publication pub1 = client.addPublication(pub1Uri, STREAM_ID);
                Publication pub2 = client.addPublication(pub2Uri, STREAM_ID))
            {
                while (pub1.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub1");
                }

                while (sub1.poll(fragmentHandler, 1) < 0)
                {
                    Tests.yieldingWait("Failed to receive from sub1");
                }

                while (pub2.offer(buffer, 0, buffer.capacity()) < 0)
                {
                    Tests.yieldingWait("Failed to publish to pub2");
                }

                while (sub2.poll(fragmentHandler, 1) < 0)
                {
                    Tests.yieldingWait("Failed to receive from sub2");
                }
            }
        }
    }
}
