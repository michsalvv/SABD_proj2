package queries.flink.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import utils.ValQ1;
import utils.ValQ2;

import java.sql.Timestamp;
import java.util.Calendar;

public class Average2 implements AggregateFunction<Tuple2<ValQ2, Integer>, AverageAccumulator2, ValQ2> {
    public AverageAccumulator2 createAccumulator() {
        return new AverageAccumulator2();
    }

    @Override
    public AverageAccumulator2 add(Tuple2<ValQ2, Integer> values, AverageAccumulator2 acc) {
        acc.sum += values.f0.getTemperature();
        acc.count++;
        acc.location = values.f0.getLocation();
        acc.last_timestamp = values.f0.getTimestamp();
        return acc;
    }

    @Override
    public AverageAccumulator2 merge(AverageAccumulator2 a, AverageAccumulator2 b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public ValQ2 getResult(AverageAccumulator2 acc) {
        double mean = acc.sum / (double) acc.count;
        ValQ2 result = new ValQ2();
        result.setLocation(acc.location);
        result.setTemperature(mean);
        result.setOccurrences(acc.count);
        result.setTimestamp(getHourSlot(acc.last_timestamp));
        return result;
    }

    public Timestamp getHourSlot(Timestamp timestamp){
        Calendar cal = Calendar.getInstance();
        cal.setTime(timestamp);
        int year = timestamp.toLocalDateTime().getYear();
        int hour = timestamp.toLocalDateTime().getHour();
        int month = timestamp.toLocalDateTime().getMonthValue();
        int day = timestamp.toLocalDateTime().getDayOfMonth();

        String ts = String.format("%d-%02d-%02d %02d:00:00", year, month, day, hour );
      return Timestamp.valueOf(ts);
    }



}