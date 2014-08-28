package uk.co.real_logic.aeron.driver;

/**
 * .
 */
public class PublicationRegistration
{
    private final DriverPublication publication;
    private final AeronClient client;
    private final long registrationId;

    public PublicationRegistration(final DriverPublication publication, final AeronClient client, final long registrationId)
    {
        this.publication = publication;
        this.client = client;
        this.registrationId = registrationId;
    }

    public void remove()
    {
        publication.decRef();
    }

    public boolean checkKeepaliveTimeout(final long now)
    {
        if (client.hasTimedOut(now))
        {
            publication.decRef();
            return true;
        }

        return false;
    }



}
