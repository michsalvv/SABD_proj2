package utils.tuples;

import com.clearspring.analytics.stream.cardinality.ICardinality;
import utils.Tools;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;

public class ValQ2 implements Serializable {
    Timestamp timestamp;
    Long location;
    Double meanTemperature;
    Long occurrences;

    public ValQ2(Timestamp timestamp, Long location, Double meanTemperature) {
        this.timestamp = timestamp;
        this.meanTemperature = meanTemperature;
        this.location = location;
    }

    public ValQ2(Timestamp timestamp, Long location, Double meanTemperature, Long occurrences) {
        this.timestamp = timestamp;
        this.meanTemperature = meanTemperature;
        this.location = location;
        this.occurrences = occurrences;
    }

    public ValQ2() {
    }

    public static ValQ2 create(String rawMessage) throws ParseException {
        var values = rawMessage.split(";");
        System.out.println("RAW: "+rawMessage);
        return new ValQ2(Timestamp.valueOf(values[0]),
                Long.parseLong(values[1]),
                Tools.stringToDouble(values[3]));
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }


    public Double getMeanTemperature() {
        if (meanTemperature == null) return 0d;
        return meanTemperature;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }


    public void setMeanTemperature(Double meanTemperature) {
        this.meanTemperature = meanTemperature;
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
                ", temperature=" + meanTemperature +
                ", occurrences=" + occurrences +
                '}';
    }

    public String toCSV() {
        return timestamp+";"+location+";"+occurrences+";"+meanTemperature;
    }

    public void addOccurrences() {
        if (occurrences == null) this.occurrences=1l;
        this.occurrences++;
    }
}
