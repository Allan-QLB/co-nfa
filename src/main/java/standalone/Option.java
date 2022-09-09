package standalone;

import java.util.Objects;

public class Option<T> {
    private final String name;
    private final Class<T> type;
    private final T defaultValue;

    private Option(String name, Class<T> type, T defaultValue) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.type = Objects.requireNonNull(type, "type cannot be null");;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public Class<T> getType() {
        return type;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    static class Builder<T> {
        private String name;
        private Class<T> type;
        private T defaultValue;

        public Builder() {
        }

        public Builder<T> name(String name) {
            this.name = name;
            return this;
        }

        public Builder<T> type(Class<T> type) {
            this.type = type;
            return this;
        }

        public Builder<T> defaultValue(T defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public Option<T> build() {
            return new Option<>(name, type, defaultValue);
        }

    }
}
