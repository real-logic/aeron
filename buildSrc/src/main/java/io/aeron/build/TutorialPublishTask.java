package io.aeron.build;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class TutorialPublishTask extends DefaultTask
{
    @Input
    public String apiKey;

    @Input
    public File source;

    @Input
    public String targetName;

    @TaskAction
    public void publish() throws Exception
    {
        final File directory = new File(getProject().getBuildDir(), "tmp/tutorialPublish");

        // Use Personal Access Token or GITHUB_TOKEN for workflows
        UsernamePasswordCredentialsProvider credentialsProvider = new UsernamePasswordCredentialsProvider(apiKey, "");

        Git git = Git.cloneRepository()
            .setURI("https://github.com/mikeb01/Aeron.wiki.git")
            .setCredentialsProvider(credentialsProvider)
            .setDirectory(directory)
            .call();

        Files.copy(
            source.toPath(),
            Paths.get(new File(directory, targetName).getAbsolutePath()),
            StandardCopyOption.REPLACE_EXISTING);

        git.add().addFilepattern(".").setUpdate(true).call();
        git.commit().setMessage("Update Docs").call();
        git.push().setCredentialsProvider(credentialsProvider).call();
    }
}