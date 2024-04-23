package io.aeron.config.validation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

final class Grep
{
    static Grep execute(final String pattern, final String sourceDir)
    {
        final String[] command = {"grep", "-r", "-n", "-E", "^" + pattern, sourceDir};
        final String commandString = "grep -r -n -E '^" + pattern + "' " + sourceDir;

        final Process process;
        try
        {
            process = new ProcessBuilder()
                .redirectErrorStream(true)
                .command(command)
                .start();
        }
        catch (final IOException e)
        {
            return new Grep(commandString, e);
        }

        final int exitCode;
        try
        {
            exitCode = process.waitFor();
        }
        catch (final InterruptedException e)
        {
            return new Grep(commandString, e);
        }

        final List<String> lines = new BufferedReader(new InputStreamReader(process.getInputStream()))
            .lines()
            .collect(Collectors.toList());

        return new Grep(commandString, exitCode, lines);
    }

    private final String commandString;

    private final int exitCode;

    private final List<String> lines;

    private final Exception e;

    private Grep(final String commandString, final Exception e)
    {
        this.commandString = commandString;
        this.exitCode = -1;
        this.lines = Collections.emptyList();
        this.e = e;
    }

    private Grep(final String commandString, final int exitCode, final List<String> lines)
    {
        this.commandString = commandString;
        this.exitCode = exitCode;
        this.lines = lines;
        this.e = null;
    }

    boolean success()
    {
        if (this.e != null)
        {
            return false;
        }

        if (this.exitCode != 0)
        {
            return false;
        }

        if (this.lines.size() != 1)
        {
            return false;
        }

        return true;
    }

    String getCommandString()
    {
        return this.commandString;
    }

    String getFilenameAndLine()
    {
        final String[] pieces = this.lines.get(0).split(":");
        return pieces[0] + ":" + pieces[1];
    }

    String getOutput()
    {
        return this.lines.get(0).split(":")[2];
    }
}
