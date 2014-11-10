package uk.co.real_logic.aeron.common;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Map;

public class UriUtil
{
    public static <M extends Map<String, String>> M parseQueryString(URI uri, M queryParams)
        throws MalformedURLException
    {
        String query = uri.getQuery();
        String[] pairs = query.split("&");
        for (String pair : pairs)
        {
            String[] componentParts = pair.split("=");
            if (componentParts.length == 2)
            {
                queryParams.put(componentParts[0], componentParts[1]);
            }
            else if (componentParts.length == 1)
            {
                queryParams.put(componentParts[0], "");
            }
            else
            {
                throw new MalformedURLException();
            }
        }

        return queryParams;
    }
}
