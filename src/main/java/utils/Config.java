package utils;

import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

public class Config {
    public static final String COMMA_DELIMITER = ";";
    //    public static final String SORTED_DATASET = "data/sorted.csv";
    public static final String SORTED_DATASET = "data/reduced.csv";
    public static final String ORIGINAL_DATASET = "data/2022-05_bmp180.csv";
    public static final String REDUCED_DATASET = "data/reduced.csv";
    public static final String ULTRA_REDUCED_DATASET = "data/ultra-reduced.csv";
    public static final int SPEEDING_FACTOR = 3600000;

    public static final int SPLIT_FACTOR = 4;
    public static final int NUM_AREAS = 16;

    // WINDOW TYPES
    public static final String HOUR = "hour";
    public static final String DAY = "day";
    public static final String WEEK = "week";
    public static final String MONTH = "month";

    public static final Long CEST = 7200000L;

    public static final OutputFileConfig outputFileConfig = OutputFileConfig.builder().withPartSuffix(".csv").build();
}
