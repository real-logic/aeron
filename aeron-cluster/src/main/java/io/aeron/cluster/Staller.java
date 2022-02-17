package io.aeron.cluster;

import java.util.concurrent.Exchanger;

public enum Staller
{
    INSTANCE;

    public Exchanger<String> exchanger = new Exchanger<>();
}
