package utils.tuples;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Comparator;

public class ValQ3 implements Serializable {
    Timestamp timestamp;
    Double mean_temp;
    Double median_temp;
    Integer cell_id;
    Long occurrences;

    public ValQ3(Timestamp timestamp, Double mean_temp, Double median_temp,
                 Integer cell_id) {
        this.timestamp = timestamp;
        this.mean_temp = mean_temp;
        this.median_temp = median_temp;
        this.cell_id = cell_id;
        this.occurrences = (long) 1;

    }

    public ValQ3() {}

    public ValQ3(Timestamp timestamp, Double mean_temp, Double median_temp, Integer cell_id, Long occurrences) {
        this.timestamp = timestamp;
        this.mean_temp = mean_temp;
        this.median_temp = median_temp;
        this.cell_id = cell_id;
        this.occurrences = occurrences;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public Double getMean_temp() {
        return mean_temp;
    }

    public void setMean_temp(Double mean_temp) {
        this.mean_temp = mean_temp;
    }

    public Double getMedian_temp() {
        return median_temp;
    }

    public void setMedian_temp(Double median_temp) {
        this.median_temp = median_temp;
    }

    public Long getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(Long occurrences) {
        this.occurrences = occurrences;
    }

    public Integer getCell_id() {
        return cell_id;
    }

    public void setCell_id(Integer cell_id) {
        this.cell_id = cell_id;
    }

    @Override
    public String toString() {
        return "ValQ3{" +
                "timestamp=" + timestamp +
                ", mean_temp=" + mean_temp +
                ", median_temp=" + median_temp +
                ", cell_id=" + cell_id +
                ", occurrences=" + occurrences +
                '}';
    }

    public static class ValQ3Comparator implements Comparator<ValQ3> {
        @Override
        public int compare(ValQ3 o1, ValQ3 o2) {
            return Double.compare(o1.getMean_temp(),o2.getMean_temp());
        }
    }
}
