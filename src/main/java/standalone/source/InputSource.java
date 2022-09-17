package standalone.source;

import standalone.StandaloneRunner;
import standalone.TimestampedElement;
import standalone.format.Format;

import java.util.concurrent.LinkedBlockingQueue;

public class InputSource<T> implements Source<T> {
    private volatile Thread produceThread;
    private final Format<T> format;
    private final LinkedBlockingQueue<TimestampedElement<T>> queue = new LinkedBlockingQueue<>();

    public InputSource(Format<T> format) {
        this.format = format;
    }

    @Override
    public void run(StandaloneRunner<T, ?> runner) {
         new Thread(() -> {
             produceThread = Thread.currentThread();
            while (!Thread.interrupted()) {
                try {
                    TimestampedElement<T> element = queue.take();
                    runner.sendElement(element);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    @Override
    public void shutdown() {
        if (produceThread != null) {
            produceThread.interrupt();
        }
    }

    public void send(long timestamp, Object data) {
        queue.offer(new TimestampedElement<>(timestamp, format.convertData(data)));
    }

}
