package io.aeron.build;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static org.eclipse.jgit.lib.ConfigConstants.CONFIG_KEY_URL;
import static org.eclipse.jgit.lib.ConfigConstants.CONFIG_REMOTE_SECTION;

public class TutorialPublishTask extends DefaultTask
{
    @Input
    public String apiKey;

    @InputDirectory
    public File source;

    @TaskAction
    public void publish() throws Exception
    {
        final String wikiUri = getWikiUri();
        final File directory = new File(getProject().getBuildDir(), "tmp/tutorialPublish");
        // Use Personal Access Token or GITHUB_TOKEN for workflows
        final CredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(apiKey, "");

        Git git = Git.cloneRepository()
            .setURI(wikiUri)
            .setCredentialsProvider(credentialsProvider)
            .setDirectory(directory)
            .call();

        final File[] asciidocFiles = AsciidocUtil.filterAsciidocFiles(source);
        System.out.println("Publishing from: " + source);
        System.out.println("Found files: " + Arrays.stream(asciidocFiles).map(File::getName).collect(joining(", ")));

        for (File asciidocFile : asciidocFiles)
        {
            Files.copy(
                asciidocFile.toPath(),
                new File(directory, asciidocFile.getName()).toPath(),
                StandardCopyOption.REPLACE_EXISTING);
        }

        git.add().addFilepattern(".").setUpdate(true).call();
        git.commit().setMessage("Update Docs").call();

        System.out.println("Publishing to: " + wikiUri);

        git.push().setCredentialsProvider(credentialsProvider).call();
    }

    public String getWikiUri() throws IOException, URISyntaxException
    {
        final File baseGitDir = new File(getProject().getRootDir(), ".git");
        if (!baseGitDir.exists() || !baseGitDir.isDirectory())
        {
            throw new IllegalStateException("Unable to find valid git repository at: " + baseGitDir);
        }

        final Repository baseGitRepo = new FileRepositoryBuilder()
            .setGitDir(new File(getProject().getRootDir(), ".git"))
            .build();

        final String origin = baseGitRepo.getConfig().getString(CONFIG_REMOTE_SECTION, "origin", CONFIG_KEY_URL);

        if (null == origin)
        {
            throw new IllegalStateException("Unable to find origin URI");
        }

        return GithubUtil.getWikiUriFromOriginUri(origin);
    }
}