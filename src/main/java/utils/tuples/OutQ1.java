package utils.tuples;

import utils.Config;
import utils.Tools;

import java.io.Serializable;
import java.sql.Timestamp;

public class OutQ1 implements Serializable, OutputQuery {
    Timestamp slotTimestamp;
    Long sensor_id;
    Double meanTemperature;
    Long occurrences;
    String header = "ts;sensor_id;count;avg_temperature\n";
    String window;

    public OutQ1(Timestamp timestamp, Long sensor_id, Double temperature) {
        this.slotTimestamp = timestamp;
        this.sensor_id = sensor_id;
        this.meanTemperature = temperature;
    }

    public OutQ1() {}

    public OutQ1(String window) {
        this.window = window;
    }

    public Timestamp getTimestamp() {
        return slotTimestamp;
    }

    public Long getSensor_id() {
        return sensor_id;
    }

    public Double getTemperature() {
        return meanTemperature;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.slotTimestamp = timestamp;
    }

    public void setSensor_id(Long sensor_id) {
        this.sensor_id = sensor_id;
    }

    public void setTemperature(Double temperature) {
        this.meanTemperature = temperature;
    }

    public Long getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(Long occurrences) {
        this.occurrences = occurrences;
    }

    @Override
    public String toString() {
        return "OutQ1{" +
                "timestamp=" + slotTimestamp +
                ", sensor_id=" + sensor_id +
                ", temperature=" + meanTemperature +
                ", occurrences=" + occurrences +
                '}';
    }

    @Override
    public String toCSV() {
        String slot = getTimestampSlot();
        System.out.println("Writing Results for Window: " + slot);
        return slot+";"+sensor_id+";"+occurrences+";"+meanTemperature;
    }

    @Override
    public String getCSVHeader() {
        return header;
    }

    public String getTimestampSlot() {
        if (window.equals(Config.HOUR)) {
            return slotTimestamp.toString().substring(0,16);
        }
        if (window.equals(Config.WEEK)) {
            return slotTimestamp.toString().substring(0,10);
        }
        if (window.equals(Config.MONTH)) {
            return slotTimestamp.toString().substring(0,7);
        }
        return slotTimestamp.toString();
    }
}
