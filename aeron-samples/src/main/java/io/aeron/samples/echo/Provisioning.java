package io.aeron.samples.echo;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Subscription;
import io.aeron.bindings.agent.api.EchoMonitorMBean;
import io.aeron.bindings.agent.api.ProvisioningConstants;
import io.aeron.bindings.agent.api.ProvisioningMBean;
import org.agrona.CloseHelper;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class Provisioning implements ProvisioningMBean
{
    private final Aeron aeron;
    private final Long2ObjectHashMap<EchoPair> echoPairByCorrelationId = new Long2ObjectHashMap<>();
    private final ManyToOneConcurrentArrayQueue<ProvisioningMessage> provisioningMessageQ =
        new ManyToOneConcurrentArrayQueue<>(1024);

    public Provisioning(Aeron aeron)
    {
        this.aeron = aeron;
    }

    public int doWork()
    {
        int workDone = 0;

        workDone += pollProvisioningQueue();
        workDone += pollEchoPairs();

        return workDone;
    }

    private int pollEchoPairs()
    {
        int workDone = 0;
        for (EchoPair echoPair : echoPairByCorrelationId.values())
        {
            workDone += echoPair.poll();
        }
        return workDone;
    }

    private int pollProvisioningQueue()
    {
        int workDone = 0;
        ProvisioningMessage poll;
        while (null != (poll = provisioningMessageQ.poll()))
        {
            workDone++;

            try
            {
                if (poll instanceof CreateEchoPair) {
                    handleCreateEchoPair((CreateEchoPair)poll);
                } else if (poll instanceof RemoveAllEchoPairs) {
                    handleRemoveAll((RemoveAllEchoPairs)poll);
                }

                poll.complete("OK");
            }
            catch (Exception e)
            {
                poll.complete(e);
            }

        }
        return workDone;
    }

    public void removeAll()
    {
        final RemoveAllEchoPairs removeAllEchoPairs = new RemoveAllEchoPairs();

        provisioningMessageQ.add(removeAllEchoPairs);

        try
        {
            removeAllEchoPairs.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void handleRemoveAll(final RemoveAllEchoPairs removeAll)
    {
        for (final EchoPair echoPair : echoPairByCorrelationId.values())
        {
            CloseHelper.quietClose(echoPair);
        }

        echoPairByCorrelationId.clear();
    }

    public void createEchoPair(
        final long correlationId,
        final String subChannel,
        final int subStreamId,
        final String pubChannel,
        final int pubStreamId)
    {
        final CreateEchoPair createEchoPair = new CreateEchoPair(
            correlationId,
            subChannel,
            subStreamId,
            pubChannel,
            pubStreamId);

        provisioningMessageQ.add(createEchoPair);

        try
        {
            createEchoPair.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void handleCreateEchoPair(final CreateEchoPair create) throws Exception
    {
        final ConcurrentPublication publication = aeron.addPublication(
            create.publicationChannel,
            create.publicationStream);

        final Subscription subscription;
        try
        {
            subscription = aeron.addSubscription(
                create.subscriptionChannel,
                create.subscriptionStream);
        }
        catch (final Exception e)
        {
            CloseHelper.quietClose(publication);
            throw e;
        }

        final EchoPair echoPair = new EchoPair(create.correlationId, subscription, publication);
        try
        {
            ManagementFactory.getPlatformMBeanServer().registerMBean(
                new StandardMBean(echoPair.monitor(), EchoMonitorMBean.class),
                new ObjectName(ProvisioningConstants.echoPairObjectName(create.correlationId)));
        }
        catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | MalformedObjectNameException e)
        {
            CloseHelper.quietCloseAll(subscription, publication);
            throw e;
        }

        echoPairByCorrelationId.put(echoPair.correlationId(), echoPair);
    }
}
