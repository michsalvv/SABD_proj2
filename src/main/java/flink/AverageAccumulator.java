package flink;

import java.sql.Timestamp;

public class AverageAccumulator {
    long count;
    double sum;
    long sensor_id;

    Timestamp last_timestamp;
}