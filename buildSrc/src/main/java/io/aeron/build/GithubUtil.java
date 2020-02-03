package io.aeron.build;

import org.eclipse.jgit.transport.URIish;

import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GithubUtil
{
    private static final Pattern PATH_PATTERN = Pattern.compile("^(.*/)?([^/]*)\\.git$");

    public static String getWikiUriFromOriginUri(String remoteUri) throws URISyntaxException
    {
        final URIish urIish = new URIish(remoteUri);

        final Matcher matcher = PATH_PATTERN.matcher(urIish.getPath());

        if (!matcher.matches())
        {
            throw new IllegalArgumentException("Invalid URI path: " + urIish.getPath());
        }

        final String path = null == matcher.group(1) ? "" : matcher.group(1);
        final String name = matcher.group(2);
        final String host = urIish.getHost();

        return "https://" + host + "/" + path + name + ".wiki.git";
    }
}
