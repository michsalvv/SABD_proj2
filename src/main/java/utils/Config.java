package utils;

import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

public class Config {
    public static final String COMMA_DELIMITER = ";";
    public static final String ORIGINAL_DATASET = "data/2022-05_bmp180.csv";
    public static final int SPEEDING_FACTOR = 360000;

    // Grid Settings
    public static final int SPLIT_FACTOR = 4;
    public static final int NUM_AREAS = 16;
    public static final Long CEST = 7200000L;

    // WINDOW TYPES
    public static final String HOUR = "hour";
    public static final String DAY = "day";
    public static final String WEEK = "week";
    public static final String MONTH = "month";
    public static final OutputFileConfig outputFileConfig = OutputFileConfig.builder().withPartSuffix(".csv").build();
}
