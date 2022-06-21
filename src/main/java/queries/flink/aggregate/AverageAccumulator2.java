package queries.flink.aggregate;

import java.sql.Timestamp;

public class AverageAccumulator2 {
    long count;
    double sum;
    long location;
    Timestamp last_timestamp;
}