package utils.tuples;

import utils.Config;

import java.sql.Timestamp;
import java.util.List;

public class OutQ2 implements OutputQuery {
    private static final String header = "ts;location1;avg_temp1;location2;avg_temp2;location3;avg_temp3;" +
            "location4;avg_temp4;location5;avg_temp5;" +
            "location6;avg_temp6;location7;avg_temp7;location8;" +
            "avg_temp8;location9;avg_temp9;location10;avg_temp10\n";
    private static final String delimiter = ";";
    private List<ValQ2> highMean;
    private List<ValQ2> lowMean;
    String window;
    Timestamp slotTimestamp;


    @Override
    public String toCSV() {
        slotTimestamp = highMean.get(0).getTimestamp();
        String slot = getTimestampSlot();
        System.out.println("Writing Results for Window: " + slot);
        StringBuilder builder = new StringBuilder();
        builder.append(slot).append(delimiter);
        for (ValQ2 val :highMean) {
            builder.append(val.getLocation()).append(delimiter);
            builder.append(val.getMeanTemperature()).append(delimiter);
        }

        for (ValQ2 val :lowMean) {
            builder.append(val.getLocation()).append(delimiter);
            builder.append(val.getMeanTemperature()).append(delimiter);
        }
        // Simply remove last useless delimiter
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }

        return builder.toString();
    }

    @Override
    public String getCSVHeader() {
        return header;
    }

    public OutQ2(List<ValQ2> high, List<ValQ2> low, String window) {
        this.highMean = high;
        this.lowMean = low;
        this.window = window;
    }

    @Override
    public String toString() {
        return "OutputQ2{" +
                "highMean=" + highMean +
                ", lowMean=" + lowMean +
                '}';
    }

    public String getTimestampSlot() {
        if (window.equals(Config.HOUR)) {
            return slotTimestamp.toString().substring(0,16);
        }
        if (window.equals(Config.DAY)) {
            return slotTimestamp.toString().substring(0,10);
        }
        if (window.equals(Config.WEEK)) {
            return slotTimestamp.toString().substring(0,10);
        }
        return slotTimestamp.toString();
    }
}
