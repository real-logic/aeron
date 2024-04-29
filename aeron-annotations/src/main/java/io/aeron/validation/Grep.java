package io.aeron.validation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class Grep
{
    public static Grep execute(final String pattern, final String sourceDir)
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

    public boolean success()
    {
        return success(true);
    }

    public boolean success(final boolean expectOneLine)
    {
        if (this.e != null)
        {
            return false;
        }

        if (this.exitCode != 0)
        {
            return false;
        }

        return !expectOneLine || this.lines.size() == 1;
    }

    public String getCommandString()
    {
        return this.commandString;
    }

    public String getFilenameAndLine()
    {
        return getFilenameAndLine(0);
    }

    public String getFilenameAndLine(final int lineNumber)
    {
        final String[] pieces = this.lines.get(lineNumber).split(":");
        return pieces[0] + ":" + pieces[1];
    }

    public String getOutput()
    {
        return getOutput(0);
    }

    public String getOutput(final int lineNumber)
    {
        return this.lines.get(lineNumber).split(":")[2];
    }

    public void forEach(final BiConsumer<String, String> action)
    {
        for (int i = 0; i < lines.size(); i++)
        {
            action.accept(getFilenameAndLine(i), getOutput(i));
        }
    }
}
