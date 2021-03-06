package utils.tuples;

import utils.Tools;

import java.io.Serializable;
import java.sql.Timestamp;

public class ValQ1 implements Serializable, OutputQuery {
    Timestamp slotTimestamp;
    Long sensor_id;
    Long occurrences;
    Double temperature;
    String header = "ts;sensor_id;count;avg_temperature\n";

    public ValQ1(String rawMessage) {
        var values = rawMessage.split(";");
        this.slotTimestamp = Timestamp.valueOf(values[0]);
        this.sensor_id = Long.parseLong(values[1]);
        this.temperature = Tools.stringToDouble(values[3]);
        this.occurrences = Long.parseLong(values[2]);
    }

    public ValQ1(Timestamp timestamp, Long sensor_id, Double temperature, Long occ) {
        this.slotTimestamp = timestamp;
        this.sensor_id = sensor_id;
        this.temperature = temperature;
        this.occurrences = occ;
    }

    public ValQ1() {
    }

    public Timestamp getTimestamp() {
        return slotTimestamp;
    }

    public Long getSensor_id() {
        return sensor_id;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.slotTimestamp = timestamp;
    }

    public void setSensor_id(Long sensor_id) {
        this.sensor_id = sensor_id;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
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
                ", temperature=" + temperature +
                ", occurrences=" + occurrences +
                '}';
    }

    @Override
    public String toCSV() { return slotTimestamp+";"+sensor_id+";"+occurrences+";"+temperature; }


    public String toSerialize() { return slotTimestamp+";"+sensor_id+";"+occurrences+";"+temperature; }

    @Override
    public String getCSVHeader() {
        return header;
    }
}
