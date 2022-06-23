package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import kafka.exception.SimulationTimeException;
import utils.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) throws InterruptedException, IOException, ParseException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-broker:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer<Object, String> producer = new KafkaProducer<>(props);

        boolean first = true;
        Timestamp previous = null;

        BufferedReader br = new BufferedReader(new FileReader(Config.REDUCED_DATASET));
        String line = br.readLine(); //skip the header
        System.out.println("Header: " + line);
        while ((line = br.readLine()) != null) {
            String[] values = line.split(Config.COMMA_DELIMITER);
            if (values.length == 10 && !values[5].isEmpty()) {
                var date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(values[5]);
                Timestamp timestamp = new Timestamp(date.getTime());
                Long sensor_id = Long.parseLong(values[0]);
                Double temperature = Double.parseDouble(values[9]);
                Long location = Long.parseLong(values[2]);
                Double latitude = Double.parseDouble(values[3]);
                Double longitude = Double.parseDouble(values[4]);
                var message = String.format("%s;%d;%,.4f;%d;%,.4f;%,.4f;", timestamp, sensor_id, temperature,
                        location, latitude, longitude);
                var producerRecord = new ProducerRecord<>("flink-events", message);
                if (first) {
                    first = false;
                } else {
                    long diff;
                    try {
                        validateTime(timestamp, previous);
                        diff = (timestamp.getTime() - previous.getTime()) / Config.SPEEDING_FACTOR;
                    } catch (SimulationTimeException e) {
                        e.printStackTrace();
                        diff = 0;
                    }
                    Thread.sleep(diff);
                }
                producer.send(producerRecord);
                System.out.printf("Send: %s%n", message);
                previous = timestamp;
            }
        }
//        producer.send(new ProducerRecord<>("flink-events", String.format("%s;%d;%,.4f;", "2026-01-01 00:00:00", 0, 0.2)));
        br.close();
    }

    static void validateTime(Timestamp actual, Timestamp previous) throws SimulationTimeException {
        long diff = (actual.getTime() - previous.getTime());
        if (diff < 0) {
            throw new SimulationTimeException("Producer Error: Timestamp out of Order");
        }
    }
}