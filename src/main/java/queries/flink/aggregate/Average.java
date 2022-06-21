package queries.flink.aggregate;

import flink.Event;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import utils.ValQ1;

import java.sql.Timestamp;
import java.util.Calendar;

public class Average implements AggregateFunction<Event, AverageAccumulator, ValQ1> {
    public AverageAccumulator createAccumulator() {
        return new AverageAccumulator();
    }

    @Override
    public AverageAccumulator add(Event values, AverageAccumulator acc) {
        acc.sum += values.getTemperature();
        acc.count++;
        acc.sensor_id = values.getSensor_id();
        acc.last_timestamp = values.getTimestamp();
        return acc;
    }

    @Override
    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public ValQ1 getResult(AverageAccumulator acc) {
        double mean = acc.sum / (double) acc.count;
        ValQ1 result = new ValQ1();
        result.setSensor_id(acc.sensor_id);
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