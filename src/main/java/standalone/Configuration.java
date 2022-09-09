package standalone;

import cn.hutool.core.collection.CollectionUtil;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;


public class Configuration {
    private final Map<String, Object> configMap;

    private Configuration(Map<String, Object> configMap) {
        this.configMap = configMap;
    }

    public static Configuration load() throws IOException {
        return new Configuration(ConfigUtil.loadConfigAsMap());
    }

    public <T> T get(@Nonnull Option<T> option) {
        final T value = get(option.getName(), option.getType());
        if (value != null) {
            return value;
        }
        return option.getDefaultValue();

    }

    public <T> T get(@Nonnull String key, @Nonnull Class<T> clazz) {
        if (CollectionUtil.isEmpty(configMap)) {
            return null;
        }
        final Object value = configMap.get(key);
        if (value == null) {
            return null;
        }
        return convert(value, clazz);
    }

    @SuppressWarnings("unchecked")
    private <T> T convert(Object value, @Nonnull Class<T> clazz) {
        if (Integer.class.equals(clazz)) {
            return (T) convertToInt(value);
        } else if (Long.class.equals(clazz)) {
            return (T) convertToLong(value);
        } else if (Boolean.class.equals(clazz)) {
            return (T) convertToBoolean(value);
        } else if (Float.class.equals(clazz)) {
            return (T) convertToFloat(value);
        } else if (Double.class.equals(clazz)) {
            return (T) convertToDouble(value);
        } else if (String.class.equals(clazz)) {
            return (T) convertToString(value);
        } else if (clazz.isEnum()) {
            return (T) convertToEnum(value, (Class<? extends Enum<?>>) clazz);
        }

        throw new IllegalArgumentException("Unsupported type: " + clazz);
    }


    private static <E extends Enum<?>> E convertToEnum(Object o, Class<E> clazz) {
        if (o.getClass().equals(clazz)) {
            return (E) o;
        }

        return Arrays.stream(clazz.getEnumConstants())
                .filter(
                        e ->
                                e.toString()
                                        .toUpperCase(Locale.ROOT)
                                        .equals(o.toString().toUpperCase(Locale.ROOT)))
                .findAny()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        String.format(
                                                "Could not parse value for enum %s. Expected one of: [%s]",
                                                clazz, Arrays.toString(clazz.getEnumConstants()))));
    }

    private static String convertToString(Object o) {
        if (o.getClass() == String.class) {
            return (String) o;
        } else if (o.getClass() == Duration.class) {
            Duration duration = (Duration) o;
            return String.format("%d ns", duration.toNanos());
        } else if (o instanceof List) {
            return ((List<?>) o)
                    .stream()
                    .map(e -> escapeWithSingleQuote(convertToString(e), ";"))
                    .collect(Collectors.joining(";"));
        } else if (o instanceof Map) {
            return ((Map<?, ?>) o)
                    .entrySet().stream()
                    .map(
                            e -> {
                                String escapedKey =
                                        escapeWithSingleQuote(e.getKey().toString(), ":");
                                String escapedValue =
                                        escapeWithSingleQuote(e.getValue().toString(), ":");

                                return escapeWithSingleQuote(
                                        escapedKey + ":" + escapedValue, ",");
                            })
                    .collect(Collectors.joining(","));
        }

        return o.toString();
    }

    static String escapeWithSingleQuote(String string, String... charsToEscape) {
        boolean escape =
                Arrays.stream(charsToEscape).anyMatch(string::contains)
                        || string.contains("\"")
                        || string.contains("'");

        if (escape) {
            return "'" + string.replaceAll("'", "''") + "'";
        }

        return string;
    }

    private static Float convertToFloat(Object o) {
        if (o.getClass() == Float.class) {
            return (Float) o;
        } else if (o.getClass() == Double.class) {
            double value = ((Double) o);
            if (value == 0.0
                    || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
                    || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
                return (float) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the float type.",
                                value));
            }
        }

        return Float.parseFloat(o.toString());
    }

    private static Double convertToDouble(Object o) {
        if (o.getClass() == Double.class) {
            return (Double) o;
        } else if (o.getClass() == Float.class) {
            return ((Float) o).doubleValue();
        }

        return Double.parseDouble(o.toString());
    }

    private static Integer convertToInt(Object o) {
        if (o.getClass() == Integer.class) {
            return (Integer) o;
        } else if (o.getClass() == Long.class) {
            long value = (Long) o;
            if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
                return (int) value;
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Configuration value %s overflows/underflows the integer type.",
                                value));
            }
        }

        return Integer.parseInt(o.toString());
    }

    private static Long convertToLong(Object o) {
        if (o.getClass() == Long.class) {
            return (Long) o;
        } else if (o.getClass() == Integer.class) {
            return ((Integer) o).longValue();
        }

        return Long.parseLong(o.toString());
    }

    private static Boolean convertToBoolean(Object o) {
        if (o.getClass() == Boolean.class) {
            return (Boolean) o;
        }

        switch (o.toString().toUpperCase()) {
            case "TRUE":
                return true;
            case "FALSE":
                return false;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Unrecognized option for boolean: %s. Expected either true or false(case insensitive)",
                                o));
        }
    }
}
