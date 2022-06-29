package utils.tuples;

import utils.Tools;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;

public class ValQ2 implements Serializable {
    Timestamp timestamp;
    Long location;
    Long occurrences;
    Double temperature;
    Double mean_temp;

    public ValQ2(String rawMessage) {
        var values = rawMessage.split(";");
        this.timestamp = Timestamp.valueOf(values[0]);
        this.location = Long.parseLong(values[1]);
        this.occurrences = Long.parseLong(values[2]);
        this.temperature = Tools.stringToDouble(values[3]);
        this.mean_temp = Tools.stringToDouble(values[4]);
    }

    public ValQ2() {
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Double getTemperature() {
        if (temperature == null) return 0D;
        return temperature;
    }

    public Double getMean_temp() {
        if (mean_temp == null) return 0D;
        return mean_temp;
    }

    public Long getOccurrences() {
        if (occurrences == null) occurrences=0L;
        return occurrences;
    }

    public Long getLocation() {
        return location;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public void setOccurrences(Long occurrences) {
        this.occurrences = occurrences;
    }

    public void setMean_temp(Double mean_temp) {
        this.mean_temp = mean_temp;
    }

    public void setLocation(Long location) {
        this.location = location;
    }

    public void calculateMean() {
        this.mean_temp = this.temperature / (double) this.occurrences;
    }

    @Override
    public String toString() {
        return "ValQ2{" +
                "timestamp=" + timestamp +
                ", location=" + location +
                ", occurrences=" + occurrences +
                ", temperature=" + temperature +
                ", mean_temp=" + mean_temp +
                '}';
    }

    public String toSerialize() { return timestamp+";"+location+";"+occurrences+";"+temperature+";"+mean_temp; }
}
