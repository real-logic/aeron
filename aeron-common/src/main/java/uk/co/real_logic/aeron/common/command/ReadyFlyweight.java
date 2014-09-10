package uk.co.real_logic.aeron.common.command;

/**
 * .
 */
public interface ReadyFlyweight
{
    int bufferOffset(int index);

    ReadyFlyweight bufferOffset(int index, int value);

    int bufferLength(int index);

    ReadyFlyweight bufferLength(int index, int value);

    String location(int index);

    ReadyFlyweight location(int index, String value);
}
