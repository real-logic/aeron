package uk.co.real_logic.aeron.tools;

/**
 * This class is responsible for holding an Aeron channel and all the stream Ids that are on it.
 * Created by bhorst on 3/4/15.
 */
public class ChannelDescriptor
{
    String channel;
    int[] streamIds;

    ChannelDescriptor()
    {
        channel = null;
        streamIds = null;
    }

    public String getChannel()
    {
        return channel;
    }

    public void setChannel(String c)
    {
        channel = c;
    }

    public int[] getStreamIdentifiers()
    {
        return streamIds;
    }

    public void setStreamIdentifiers(int[] ids)
    {
        streamIds = ids;
    }

    public String toString()
    {
        return channel;
    }
}