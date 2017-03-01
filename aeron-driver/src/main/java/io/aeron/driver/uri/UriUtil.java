/*
 * Copyright 2014-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver.uri;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Utility functions for dealing with {@link URI}s.
 */
public class UriUtil
{
    /**
     * Parse a URI query string and extract the params as key value pairs.
     *
     * @param uri         with query string.
     * @param queryParams to be populated.
     * @param <M> container for the params
     * @return the queryParams that has been populated.
     * @throws URISyntaxException if a parsing exception occurs on the query string.
     */
    public static <M extends Map<String, String>> M parseQueryString(final URI uri, final M queryParams)
        throws URISyntaxException
    {
        final String query = uri.getQuery();

        if (null != query)
        {
            for (final String pair : query.split("&"))
            {
                final String[] componentParts = pair.split("=");
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
                    throw new URISyntaxException(pair, "Did not contain 1 or 2 parts");
                }
            }
        }

        return queryParams;
    }
}
