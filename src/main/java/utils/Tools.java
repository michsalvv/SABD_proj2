package utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

public class Tools {

    public static Double stringToDouble(String s) {
        String r = s.replace(",",".");
        return Double.parseDouble(r);
    }

    public static List<ValQ2> getTopFiveLocations(Iterable<ValQ2> list) {
        List<ValQ2> top = new ArrayList<>();
        List<Long> topId = new ArrayList<>();
        int n = 0;

        while (n!=5) {
            Iterator<ValQ2> iterator = list.iterator();
            ValQ2 max = null;
            Double maxVal = 0D;
            Long maxId = 0L;

            while (iterator.hasNext()) {
                ValQ2 element = iterator.next();

                ValQ2 actual = element;
                Double actualVal = actual.getTemperature();
                Long actualId = actual.getLocation();
                if (actualVal >= maxVal && !topId.contains(actualId)) {
                    maxVal = actualVal;
                    max = actual;
                    maxId = actualId;
                }
            }
            n++;
            topId.add(maxId);
            top.add(max);
        }
        return top;
    }

    public static Timestamp getHourSlot(Timestamp timestamp){
        Calendar cal = Calendar.getInstance();
        cal.setTime(timestamp);
        int year = timestamp.toLocalDateTime().getYear();
        int hour = timestamp.toLocalDateTime().getHour();
        int month = timestamp.toLocalDateTime().getMonthValue();
        int day = timestamp.toLocalDateTime().getDayOfMonth();

        String ts = String.format("%d-%02d-%02d %02d:00:00", year, month, day, hour );
        return Timestamp.valueOf(ts);
    }

    public static Timestamp getMinutesSlot(Timestamp timestamp, int minutes){
        Calendar cal = Calendar.getInstance();
        cal.setTime(timestamp);
        int year = timestamp.toLocalDateTime().getYear();
        int hour = timestamp.toLocalDateTime().getHour();
        int month = timestamp.toLocalDateTime().getMonthValue();
        int day = timestamp.toLocalDateTime().getDayOfMonth();
        int minute = timestamp.toLocalDateTime().getMinute();
        int ceil = (int) Math.ceil((double) minute / minutes);
        int slot = ceil * minutes;

        String ts = String.format("%d-%02d-%02d %02d:%02d:00", year, month, day, hour, slot);
        return Timestamp.valueOf(ts);
    }

    public static Timestamp getSecondsSlot(Timestamp timestamp, int seconds){
        Calendar cal = Calendar.getInstance();
        cal.setTime(timestamp);
        int year = timestamp.toLocalDateTime().getYear();
        int hour = timestamp.toLocalDateTime().getHour();
        int month = timestamp.toLocalDateTime().getMonthValue();
        int day = timestamp.toLocalDateTime().getDayOfMonth();
        int minute = timestamp.toLocalDateTime().getMinute();
        int second = timestamp.toLocalDateTime().getSecond();
        int ceil = second/seconds;
        int slot = ceil * seconds;

        String ts = String.format("%d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, slot);
        return Timestamp.valueOf(ts);
    }
}
