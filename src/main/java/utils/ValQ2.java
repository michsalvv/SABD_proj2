package utils;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

public class ValQ2 implements Serializable {
    Timestamp timestamp;
    Long location;
    Double temperature;
    Long occurrences;

    public ValQ2(Timestamp timestamp, Long location, Double temperature) {
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.location = location;
    }

    public ValQ2() {
    }

    public static ValQ2 create(String rawMessage) throws ParseException {
        var values = rawMessage.split(";");
        return new ValQ2(Timestamp.valueOf(values[0]),
                Long.parseLong(values[3]),
                utils.Tools.stringToDouble(values[2]));
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }


    public Double getTemperature() {
        return temperature;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
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

    public Long getLocation() {
        return location;
    }

    public void setLocation(Long location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "ValQ2{" +
                "timestamp=" + timestamp +
                ", location=" + location +
                ", temperature=" + temperature +
                ", occurrences=" + occurrences +
                '}';
    }
}
