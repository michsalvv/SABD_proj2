package utils;

import flink.KafkaConnectorDemo;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

public class ValQ1 implements Serializable {
    Timestamp timestamp;

    Long sensor_id;
    Double temperature;

    public ValQ1(Timestamp timestamp, Long sensor_id, Double temperature) {
        this.timestamp = timestamp;
        this.sensor_id = sensor_id;
        this.temperature = temperature;
    }

    public static ValQ1 create(String rawMessage) throws ParseException {
        var values = rawMessage.split(";");
        System.out.println(values[0]);
        System.out.println(values[1]);
        System.out.println(values[2]);
        return new ValQ1(Timestamp.valueOf(values[0]), Long.parseLong(values[1]),
                NumberFormat.getInstance(Locale.getDefault()).parse(values[2]).doubleValue());
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

    @Override
    public String toString() {
        return "ValQ1{" +
                "timestamp=" + timestamp +
                ", sensor_id=" + sensor_id +
                ", temperature=" + temperature +
                '}';
    }

}
