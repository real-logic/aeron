/*
 * Copyright 2014-2024 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.build;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ObjectReader;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.URIish;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

final class GithubUtil
{
    private GithubUtil()
    {
    }

    public static String currentGitHash(final String projectDir) throws IOException, GitAPIException
    {
        final FileRepositoryBuilder repositoryBuilder = new FileRepositoryBuilder().findGitDir(new File(projectDir));
        if (repositoryBuilder.getGitDir() == null)
        {
            // No .git directory. That will be the case when people download the repo as a zip file.
            return "unknown";
        }

        try (Repository repository = repositoryBuilder.build();
            ObjectReader reader = repository.newObjectReader();
            Git git = new Git(repository))
        {
            final RevCommit commit = git.log().setMaxCount(1).call().iterator().next();
            final ObjectId commitId = commit.toObjectId();
            final String commitSha = reader.abbreviate(commitId, 10).name();
            final Status status = git.status().call();

            return status.hasUncommittedChanges() ? commitSha + "+guilty" : commitSha;
        }
    }

    static String getWikiUriFromOriginUri(final String remoteUri) throws URISyntaxException
    {
        final URIish urIish = new URIish(remoteUri);
        final String uriPath = urIish.getPath();

        if (uriPath.endsWith("/"))
        {
            throw new IllegalArgumentException("unable to handle URI path ending in '/': " + remoteUri);
        }

        final int lastSlashIndex = urIish.getPath().lastIndexOf('/');

        final String path = lastSlashIndex == -1 ? "" : uriPath.substring(0, lastSlashIndex + 1);
        final String prefixedPath = path.startsWith("/") ? path : "/" + path;
        final String repoName = lastSlashIndex == -1 ? uriPath : uriPath.substring(lastSlashIndex + 1);
        final String name = stripSuffix(repoName, ".git");
        final String host = stripSuffix(urIish.getHost(), "/");

        final String wikiUri = "https://" + host + prefixedPath + name + ".wiki.git";

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
