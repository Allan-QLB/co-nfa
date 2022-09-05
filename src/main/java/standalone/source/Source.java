package standalone.source;


import standalone.StandaloneRunner;

public interface Source {
    void run(StandaloneRunner<?> runner) throws Exception;

    void shutdown();
}
