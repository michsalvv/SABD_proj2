package utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Tools {

    public static Double stringToDouble(String s) {
        String r = s.replace(",",".");
        return Double.parseDouble(r);
    }

    // es. 40° appartiene a (30°, 45°) = (first, last)
    public static boolean inRange(Double val, Double first, Double last) {
        if (Double.compare(val, first) >= 0 && Double.compare(val, last) <= 0) {
            return true;
        }
        return false;
    }

    public static List<ValQ2> getTopFiveLocations(Iterable<ValQ2> list) {
        System.out.println("ELEMENTS:");
        list.forEach(r-> System.out.println(r));

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
}
