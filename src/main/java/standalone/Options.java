package standalone;

public class Options {
    public static final Option<String> TMP_DIR = Option.<String>builder()
            .name("tmp.dir")
            .type(String.class)
            .defaultValue("/tmp")
            .build();

    public static final Option<LocalStateStorage> LOCAL_STATE_STORAGE = Option.<LocalStateStorage>builder()
            .name("local.storage")
            .type(LocalStateStorage.class)
            .defaultValue(LocalStateStorage.HEAP)
            .build();

}
