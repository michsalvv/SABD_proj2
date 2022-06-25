package utils.tuples;

import java.io.Serializable;
import java.sql.Timestamp;

public class ValQ1 implements Serializable, OutputQuery {
    Timestamp slotTimestamp;
    Long sensor_id;
    Double meanTemperature;
    Long occurrences;
    String header = "ts;sensor_id;count;avg_temperature\n";
    public ValQ1(Timestamp timestamp, Long sensor_id, Double temperature) {
        this.slotTimestamp = timestamp;
        this.sensor_id = sensor_id;
        this.meanTemperature = temperature;
    }

    public ValQ1() {
    }

    public ValQ1(Timestamp slotTimestamp, Long sensor_id, Double meanTemperature, Long occurrences) {
        this.slotTimestamp = slotTimestamp;
        this.sensor_id = sensor_id;
        this.meanTemperature = meanTemperature;
        this.occurrences = occurrences;
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
        return "ValQ1{" +
                "timestamp=" + slotTimestamp +
                ", sensor_id=" + sensor_id +
                ", temperature=" + meanTemperature +
                ", occurrences=" + occurrences +
                '}';
    }

    @Override
    public String toCSV() {
        return slotTimestamp+";"+sensor_id+";"+occurrences+";"+meanTemperature;
    }

    @Override
    public String getCSVHeader() {
        return header;
    }
}
