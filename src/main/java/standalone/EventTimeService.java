package standalone;

import java.util.ArrayList;
import java.util.List;

public class EventTimeService {

    private final List<WatermarkListener> watermarkListeners = new ArrayList<>();
    public long currentWatermark;

    public long currentWatermark() {
        return currentWatermark;
    }

    public void tryAdvanceWatermark(TimestampedElement<?> element) throws Exception {
        final long watermark = element.getTimestamp();
        if (watermark > currentWatermark) {
            currentWatermark = watermark /* - 120_000*/;
            for (WatermarkListener watermarkListener : watermarkListeners) {
                watermarkListener.onWatermark(watermark);
            }
        }

    }



    public void addListener(WatermarkListener listener) {
        this.watermarkListeners.add(listener);
    }


}
