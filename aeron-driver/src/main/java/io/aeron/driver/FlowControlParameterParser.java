package io.aeron.driver;

import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import static org.agrona.AsciiEncoding.parseIntAscii;
import static org.agrona.AsciiEncoding.parseLongAscii;
import static org.agrona.SystemUtil.parseDuration;

class FlowControlParameterParser
{
    static void parse(
        final String fcValue,
        final LongConsumer receiverTimeoutNs,
        final IntConsumer groupMinSize,
        final LongConsumer groupReceiverTag)
    {
        if (null != fcValue)
        {
            for (final String arg : fcValue.split(","))
            {
                if (arg.startsWith("t:"))
                {
                    receiverTimeoutNs.accept(parseDuration("fc receiver timeout", arg.substring(2)));
                }
                else if (arg.startsWith("g:"))
                {
                    final int groupMinSizeIndex = arg.indexOf('/');

                    if (2 != groupMinSizeIndex)
                    {
                        final int lengthToParse = -1 == groupMinSizeIndex ?
                            arg.length() - 2 : groupMinSizeIndex - 2;
                        groupReceiverTag.accept(parseLongAscii(arg, 2, lengthToParse));
                    }

                    if (-1 != groupMinSizeIndex)
                    {
                        groupMinSize.accept(parseIntAscii(
                            arg, groupMinSizeIndex + 1, arg.length() - (groupMinSizeIndex + 1)));
                    }
                }
            }
        }
    }
}
