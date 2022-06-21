package utils;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

public class ValQ1 implements Serializable {
    Timestamp timestamp;

    Long sensor_id;
    Double temperature;
    Long occurrences;

    public ValQ1(Timestamp timestamp, Long sensor_id, Double temperature) {
        this.timestamp = timestamp;
        this.sensor_id = sensor_id;
        this.temperature = temperature;
    }

    public ValQ1() {
    }

    public static ValQ1 create(String rawMessage) throws ParseException {
        var values = rawMessage.split(";");
        return new ValQ1(Timestamp.valueOf(values[0]), Long.parseLong(values[1]),
                utils.Tools.stringToDouble(values[2]));
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Long getSensor_id() {
        return sensor_id;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
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
                "timestamp=" + timestamp +
                ", sensor_id=" + sensor_id +
                ", temperature=" + temperature +
                ", occurrences=" + occurrences +
                '}';
    }

}
