package uk.co.real_logic.aeron.common.uri;

public class AeronUri
{
    private static final String AERON_PREFIX = "aeron:";
    private final String scheme;
    private final String media;

    public AeronUri(String scheme, String media)
    {
        this.scheme = scheme;
        this.media = media;
    }

    public String getMedia()
    {
        return media;
    }

    public String getScheme()
    {
        return scheme;
    }

    private enum State
    {
        MEDIA, DONE
    }

    public static AeronUri parse(CharSequence cs)
    {
        if (!startsWith(cs, AERON_PREFIX))
        {
            throw new IllegalArgumentException("AeronUri must start with 'aeron:', found: '" + cs + "'");
        }

        final StringBuilder builder = new StringBuilder();

        final String scheme = "aeron";
        State state = State.MEDIA;

        String media = null;

        for (int i = AERON_PREFIX.length(); i < cs.length(); i++)
        {
            final char c = cs.charAt(i);

            switch (state)
            {
            case MEDIA:
                switch (c)
                {
                case ':':
                    media = builder.toString();
                    builder.setLength(0);
                    state = State.DONE;
                    break;

                default:
                    builder.append(c);
                }
                break;

            case DONE:
                throw new IllegalStateException("Was done, but received more input");

            default:
                throw new IllegalStateException("Que?  State = " + state);
            }
        }

        switch (state)
        {
        case MEDIA:
            media = builder.toString();
            break;

        default:
            // No-op
        }

        return new AeronUri(scheme, media);
    }

    private static boolean startsWith(CharSequence input, CharSequence prefix)
    {
        if (input.length() < prefix.length())
        {
            return false;
        }

        for (int i = 0; i < prefix.length(); i++)
        {
            if (input.charAt(i) != prefix.charAt(i))
            {
                return false;
            }
        }

        return true;
    }
}
