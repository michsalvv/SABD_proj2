package utils;

import utils.tuples.*;
import utils.tuples.ValQ3.ValQ3Comparator;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Tools {

    public static Double stringToDouble(String s) {
        String r = s.replace(",",".");
        return Double.parseDouble(r);
    }

    public static boolean inRange(Double val, Double first, Double last) {
        if (Double.compare(val, first) >= 0 && Double.compare(val, last) <= 0) {
            return true;
        }
        return false;
    }

    public static OutputQ2 getLocationsRanking(Iterable<ValQ2> list) {
        List<ValQ2> high = new ArrayList<>();
        List<Long> highIds = new ArrayList<>();
        List<ValQ2> low = new ArrayList<>();
        List<Long> lowIds = new ArrayList<>();
        int n = 0;

        while (n!=5) {
            Iterator<ValQ2> iterator = list.iterator();
            ValQ2 max = null;
            Double maxVal = -999999D;
            Long maxId = 0L;

            ValQ2 min = null;
            Double minVal = 999999D;
            Long minId = 0L;

            while (iterator.hasNext()) {
                ValQ2 actual = iterator.next();

                Double actualVal = actual.getMeanTemperature();
                Long actualId = actual.getLocation();
                if (actualVal >= maxVal && !highIds.contains(actualId)) {
                    maxVal = actualVal;
                    max = actual;
                    maxId = actualId;
                }
                if (actualVal <= minVal && !lowIds.contains(actualId)) {
                    minVal = actualVal;
                    min = actual;
                    minId = actualId;
                }
            }
            n++;
            highIds.add(maxId);
            high.add(max);
            lowIds.add(minId);
            low.add(min);
        }
        return new OutputQ2(high,low);
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

    /**
     * Returns a sorted copy of the provided collection of things. Uses the natural ordering of the things.
     */
    public static List<ValQ3> sortByTemperature (Iterable<ValQ3> things) {
        ValQ3Comparator comparator = new ValQ3Comparator();
        List<ValQ3> copy = toMutableList(things);
        Collections.sort(copy, comparator);
        return copy;
    }


    private static <ValQ3> List<ValQ3> toMutableList(Iterable<ValQ3> things) {
        if (things == null) {
            return new ArrayList<>(0);
        }
        List<ValQ3> list = new ArrayList<ValQ3>();
        for (ValQ3 thing : things) {
            list.add(thing);
        }
        return list;
    }

    public static OutputQuery cellStatisticsOnRow(Iterable<ValQ3> iterable) {
        List<ValQ3> rows = new ArrayList<>();
        Iterator<ValQ3> iterator = iterable.iterator();
        for (int i = 0; i<16; i++){
            rows.add(new ValQ3(new Timestamp(0),0D,0D,-1));
        }

        while (iterator.hasNext()) {
            ValQ3 actual = iterator.next();
            Integer pos = actual.getCell_id();
            rows.set(pos,actual);
        }

        return new OutputQ3(rows);
    }
}
