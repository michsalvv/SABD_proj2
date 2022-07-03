package utils.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.queries.LocationAggregator;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class LocationAggregatorSerde implements Serializer<LocationAggregator>, Deserializer<LocationAggregator> {
    public LocationAggregatorSerde() { }

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public LocationAggregator deserialize(String topic, byte[] data) {
        if (data == null) return null;

        LocationAggregator finalObj;
        try {
            finalObj = objectMapper.treeToValue(objectMapper.readTree(data), LocationAggregator.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return finalObj;
    }

    @Override
    public LocationAggregator deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public byte[] serialize(String topic, LocationAggregator data) {
        if (data == null){
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, LocationAggregator data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
