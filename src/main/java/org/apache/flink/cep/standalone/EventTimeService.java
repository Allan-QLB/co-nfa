package org.apache.flink.cep.standalone;

import org.apache.flink.runtime.state.VoidNamespace;

import java.util.List;
import java.util.PriorityQueue;

public class EventTimeService {

    private List<WatermarkListener> watermarkListeners;
    public long currentWatermark;

    public long currentWatermark() {
        return currentWatermark;
    }

    public void tryAdvanceWatermark(TimestampedElement<?> element) throws Exception {
        final long watermark = element.getTimestamp() - 120_000;
        if (watermark > currentWatermark) {
            currentWatermark = watermark;
            for (WatermarkListener watermarkListener : watermarkListeners) {
                watermarkListener.onWatermark(watermark);
            }
        }

    }



    public void addListener(WatermarkListener listener) {
        this.watermarkListeners.add(listener);
    }


}
