package kafka.queries;

import utils.tuples.OutputQuery;
import utils.tuples.ValQ2;

import java.io.Serializable;
import java.util.*;

// Non togliere i getter e setter altrimenti non serializza
public class LocationAggregator implements Serializable {

    private static final String DELIMITER = ";";
    TreeMap<Double, Long> topLocations = new TreeMap<>();
    TreeMap<Double, Long> lowLocations = new TreeMap<>();

    String timestamp;

    public LocationAggregator() {
    }


    public void updateRank(ValQ2 valQ2) {

        if (topLocations.size() < 5) {
            topLocations.put(valQ2.getMean_temp(), valQ2.getLocation());

            if (lowLocations.size() < 5) {
                lowLocations.put(valQ2.getMean_temp(), valQ2.getLocation());
            }
            return;
        }

        //else
        updateTops(valQ2);
        updateLows(valQ2);

    }

    public void updateTops(ValQ2 val) {
        //TOP:      2,3,4,5,8
        if (topLocations.containsKey(val.getMean_temp())) return;

        Double lowestTopMean = topLocations.firstKey();         // = 2
        if (val.getMean_temp() < lowestTopMean) return;

        topLocations.put(val.getMean_temp(), val.getLocation());
        topLocations.remove(topLocations.firstKey());
    }

    public void updateLows(ValQ2 val) {
//        LOW:      2,3,4,5,8
        if (lowLocations.containsKey(val.getMean_temp())) return;

        Double highestLowMean = lowLocations.lastKey();
        if (val.getMean_temp() > highestLowMean) return;

        lowLocations.put(val.getMean_temp(), val.getLocation());
        lowLocations.remove(lowLocations.lastKey());
    }

    public TreeMap<Double, Long> getTopLocations() {
        return topLocations;
    }

    public void setTopLocations(TreeMap<Double, Long> topLocations) {
        this.topLocations = topLocations;
    }

    public TreeMap<Double, Long> getLowLocations() {
        return lowLocations;
    }

    public void setLowLocations(TreeMap<Double, Long> lowLocations) {
        this.lowLocations = lowLocations;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LocationAggregator{" +
                "topLocations=" + topLocations.descendingMap() +
                ", lowLocations=" + lowLocations +
                '}';
    }


    public String toSerialize() {

        StringBuilder builder = new StringBuilder();
        topLocations.descendingMap()
                .forEach((temp, key) -> {
                    builder.append(key).append(DELIMITER);
                    builder.append(temp).append(DELIMITER);
                });

        String topMeans = builder.toString();
        builder.setLength(0);

        lowLocations.forEach((temp, key) -> {
            builder.append(key).append(DELIMITER);
            builder.append(temp).append(DELIMITER);
        });
        String lowMeans = builder.toString();

        return timestamp + ";"
                + topMeans
                + lowMeans.substring(0, lowMeans.length() - 1);
    }
}
