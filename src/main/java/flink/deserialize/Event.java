package flink.deserialize;

import utils.Tools;

import java.io.Serializable;
import java.sql.Timestamp;

public class Event implements Serializable {
    private Timestamp timestamp;
    private Long sensor_id;
    private Double temperature;
    private Long location;
    private Double latitude;
    private Double longitude;

    public Event(String rawMessage) {
        var values = rawMessage.split(";");
        this.timestamp = Timestamp.valueOf(values[0]);
        this.sensor_id = Long.parseLong(values[1]);
        this.temperature = Tools.stringToDouble(values[2]);
        this.latitude = Tools.stringToDouble(values[4]);
        this.longitude = Tools.stringToDouble(values[5]);
        this.location = Long.parseLong(values[3]);
    }

    @Override
    public String toString() {
        return "Event{" +
                "timestamp=" + timestamp +
                ", sensor_id=" + sensor_id +
                ", temperature=" + temperature +
                ", location=" + location +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }
    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public Long getSensor_id() {
        return sensor_id;
    }

    public void setSensor_id(Long sensor_id) {
        this.sensor_id = sensor_id;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public Long getLocation() {
        return location;
    }

    public void setLocation(Long location) {
        this.location = location;
    }

    public Double getLatitude() { return latitude; }

    public void setLatitude(Double latitude) { this.latitude = latitude; }

    public Double getLongitude() { return longitude; }

    public void setLongitude(Double longitude) { this.longitude = longitude; }
}