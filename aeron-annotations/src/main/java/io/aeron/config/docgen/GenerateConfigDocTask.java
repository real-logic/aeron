package io.aeron.config.docgen;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.aeron.config.ConfigInfo;
import io.aeron.config.DefaultType;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GenerateConfigDocTask
{
    private static FileWriter writer;

    public static void main(final String[] args) throws Exception
    {
        try (FileWriter writer = new FileWriter(args[1]))
        {
            GenerateConfigDocTask.writer = writer;

            final List<ConfigInfo> config = fetchConfig(args[0])
                .stream()
                .sorted(Comparator.comparing(a -> a.id))
                .collect(Collectors.toList());

            for (final ConfigInfo configInfo: config)
            {
                writeHeader(toHeaderString(configInfo.id));
                write("Description", configInfo.propertyNameDescription);
                write("Type",
                    (DefaultType.isUndefined(configInfo.overrideDefaultValueType) ?
                    configInfo.defaultValueType :
                    configInfo.overrideDefaultValueType).getSimpleName());
                writeCode("System Property", configInfo.propertyName);
                if (configInfo.context != null && !configInfo.context.isEmpty())
                {
                    writeCode("Context", configInfo.context);
                }
                else
                {
                    System.err.println("missing context for " + configInfo.id);
                }
                if (configInfo.uriParam != null && !configInfo.uriParam.isEmpty())
                {
                    writeCode("URI Param", configInfo.uriParam);
                }
                write("Default", getDefaultString(
                    configInfo.overrideDefaultValue == null ?
                    configInfo.defaultValue.toString() :
                    configInfo.overrideDefaultValue.toString()));
                if (configInfo.expectations.c.exists)
                {
                    writeCode("C Env Var", configInfo.expectations.c.envVar);
                    write("C Default", getDefaultString(
                        configInfo.expectations.c.defaultValue.toString()));
                }
                writeLine();
            }
        }
        catch (final IOException e)
        {
            e.printStackTrace(System.err);
        }
        finally
        {
            GenerateConfigDocTask.writer = null;
        }
    }

    private static List<ConfigInfo> fetchConfig(final String configInfoFilename) throws Exception
    {
        return new ObjectMapper().readValue(
            Paths.get(configInfoFilename).toFile(),
            new TypeReference<List<ConfigInfo>>()
            {
            });
    }


    private static void writeHeader(final String t) throws IOException
    {
        writeRow("", t);
        writeLine();
        writeRow("---", "---");
        writeLine();
    }

    private static void writeCode(final String a, final String b) throws IOException
    {
        write(a, "`" + b + "`");
    }

    private static void write(final String a, final String b) throws IOException
    {
        writeRow("**" + a + "**", b.replaceAll("\n", " "));
        writeLine();
    }

    private static void writeLine() throws IOException
    {
        writer.write("\n");
    }

    private static void writeRow(final String a, final String b) throws IOException
    {
        writer.write("| " + a + " | " + b + " |");
    }

    private static String toHeaderString(final String t)
    {
        final StringBuilder builder = new StringBuilder();

        char previous = '_';
        for (final char next: t.toCharArray())
        {
            if (next == '_')
            {
                builder.append(' ');
            }
            else if (previous == '_')
            {
                builder.append(Character.toUpperCase(next));
            }
            else
            {
                builder.append(Character.toLowerCase(next));
            }
            previous = next;
        }
        return builder.toString();
    }

    private static String getDefaultString(final String d)
    {
        if (d != null && d.length() > 3 && d.chars().allMatch(Character::isDigit))
        {
            final long defaultLong = Long.parseLong(d);

            final StringBuilder builder = new StringBuilder();
            builder.append(d);
            builder.append(" (");
            builder.append(DecimalFormat.getNumberInstance().format(defaultLong));
            builder.append(")");

            int kCount = 0;
            long remainingValue = defaultLong;
            while (remainingValue % 1024 == 0)
            {
                kCount++;
                remainingValue = remainingValue / 1024;
            }

            if (kCount > 0 && remainingValue < 1024)
            {
                builder.append(" (");
                builder.append(remainingValue);
                IntStream.range(0, kCount).forEach(i -> builder.append(" * 1024"));
                builder.append(")");
            }

            return builder.toString();

            /* TODO if there were some way to know that this was a timeout of some sort, we could
            indicate the number of (milli|micro|nano)seconds
             */
        }
        return d;
    }
}
