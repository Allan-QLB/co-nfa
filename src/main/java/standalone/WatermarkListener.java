package standalone;

@FunctionalInterface
public interface WatermarkListener {
    void onWatermark(long watermark) throws Exception;
}
