package io.aeron.build;

import org.eclipse.jgit.transport.URIish;

import java.net.URISyntaxException;
import java.util.StringJoiner;
import java.util.regex.Pattern;

public class GithubUtil
{
    private static final Pattern PATH_PATTERN = Pattern.compile("^(.*/)?([^/]*)(\\.git$)?");

    public static String getWikiUriFromOriginUri(String remoteUri) throws URISyntaxException
    {
        final URIish urIish = new URIish(remoteUri);
        final String uriPath = urIish.getPath();

        if (uriPath.endsWith("/"))
        {
            throw new IllegalArgumentException("Unable to handle URI path ending in '/': " + remoteUri);
        }

        final int lastSlashIndex = urIish.getPath().lastIndexOf('/');

        final String path = lastSlashIndex == -1 ? "" : uriPath.substring(0, lastSlashIndex + 1);
        final String repoName = lastSlashIndex == -1 ? uriPath : uriPath.substring(lastSlashIndex + 1);
        final String name = stripSuffix(repoName, ".git");
        final String host = stripSuffix(urIish.getHost(), "/");

        final String wikiUri = "https://" + host + "/" + path + name + ".wiki.git";

        System.out.println("Origin: " + remoteUri);
        System.out.println("Wiki  : " + wikiUri);

        return wikiUri;
    }

    private static String stripSuffix(final String s, final String suffix)
    {
        if (s.endsWith(suffix))
        {
            return s.substring(0, s.length() - suffix.length());
        }

        return s;
    }
}
