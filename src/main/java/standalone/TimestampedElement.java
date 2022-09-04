package standalone;

public class TimestampedElement<E> {
    private final long timestamp;
    private final E element;

    public TimestampedElement(long timestamp, E element) {
        this.timestamp = timestamp;
        this.element = element;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public E getElement() {
        return element;
    }
}
