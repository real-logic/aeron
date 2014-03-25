package uk.co.real_logic.aeron.benchmark.filelocks;

public interface Signaller
{

    void await();

    void start();

    void signal();

}
