package standalone.source;


import standalone.StandaloneRunner;

public interface Source<T> {
    void run(StandaloneRunner<T, ?> runner) throws Exception;

    void shutdown();
}
