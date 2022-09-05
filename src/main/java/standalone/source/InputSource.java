package standalone.source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import standalone.StandaloneRunner;
import standalone.TimestampedElement;

import java.util.concurrent.LinkedBlockingQueue;

public class InputSource implements Source {
    private volatile Thread produceThread;
    private final LinkedBlockingQueue<TimestampedElement<JSONObject>> queue = new LinkedBlockingQueue<>();
    @Override
    public void run(StandaloneRunner<?> runner) {
         new Thread(() -> {
             produceThread = Thread.currentThread();
            while (!Thread.interrupted()) {
                try {
                    TimestampedElement<JSONObject> element = queue.take();
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
        queue.offer(new TimestampedElement<>(timestamp, (JSONObject) JSON.toJSON(data)));
    }

}
