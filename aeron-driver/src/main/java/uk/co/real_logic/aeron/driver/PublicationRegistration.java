package uk.co.real_logic.aeron.driver;

/**
 * Tracks a aeron client interest registration in a {@link DriverPublication}.
 */
public class PublicationRegistration
{
    private final DriverPublication publication;
    private final AeronClient client;

    public PublicationRegistration(final DriverPublication publication, final AeronClient client)
    {
        this.publication = publication;
        this.client = client;
    }

    public void remove()
    {
        publication.decRef();
    }

    public boolean hasClientTimedOut(final long now)
    {
        if (client.hasTimedOut(now))
        {
            publication.decRef();
            return true;
        }

        return false;
    }
}
