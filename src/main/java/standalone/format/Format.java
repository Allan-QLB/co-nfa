package standalone.format;

public interface Format<T> {
    T convertData(Object data);
}
