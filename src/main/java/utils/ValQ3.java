package utils;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.ParseException;

public class ValQ3 implements Serializable {
    Timestamp timestamp;
    Long sensor_id;
    Double latitude;
    Double longitude;
    Double mean_temp;
    Double median_temp;
    Long occurrences;
    Integer cell_id;
    Grid.Vertex top_left;
    Grid.Vertex bottom_right;

    public ValQ3(Timestamp timestamp, Long sensor_id, Double latitude, Double longitude, Double mean_temp, Double median_temp,
                 Integer cell_id, Grid.Vertex top_left, Grid.Vertex bottom_right) {
        this.timestamp = timestamp;
        this.sensor_id = sensor_id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.mean_temp = mean_temp;
        this.median_temp = median_temp;
        this.occurrences = (long)1;
        this.cell_id = cell_id;
        this.top_left = top_left;
        this.bottom_right = bottom_right;
    }

    public ValQ3(Integer cell_id) {
        this.cell_id = cell_id;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
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

    public void setCell_id(Integer cell_id) { this.cell_id = cell_id; }

    public Grid.Vertex getTop_left() {
        return top_left;
    }

    public void setTop_left(Grid.Vertex top_left) {
        this.top_left = top_left;
    }

    public Grid.Vertex getBottom_right() {
        return bottom_right;
    }

    public void setBottom_right(Grid.Vertex bottom_right) {
        this.bottom_right = bottom_right;
    }

    @Override
    public String toString() {
        return "ValQ3{" +
                "timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", mean_temp=" + mean_temp +
                ", median_temp=" + median_temp +
                ", occurrences=" + occurrences +
                ", cell_id=" + cell_id +
                ", top_left=(" + top_left.getLat() + "," + top_left.getLon() + ")" +
                ", bottom_right=(" + bottom_right.getLat() + "," + bottom_right.getLon() + ")" +
                '}';
    }
}
