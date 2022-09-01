package org.apache.flink.cep.standalone;

@FunctionalInterface
public interface WatermarkListener {
    void onWatermark(long watermark) throws Exception;
}
