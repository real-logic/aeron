package uk.co.real_logic.aeron.common;

public class Strings
{
    public static boolean isEmpty(String s)
    {
        return null == s || s.isEmpty();
    }

    public static int parseIntOrDefault(String s, int defaultValue)
    {
        if (null == s)
        {
            return defaultValue;
        }

        return Integer.parseInt(s);
    }
}
