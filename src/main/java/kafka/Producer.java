package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

public class Producer {
    private static final String COMMA_DELIMITER = ";";

    public static void main(String[] args) throws InterruptedException, IOException, ParseException {
        Integer speed_factor = null;

        //properties and creation of a Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-broker:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.Producer<Object, String> producer = new KafkaProducer<>(props);
        boolean first = true;
        Timestamp previous = null;
        BufferedReader br = new BufferedReader(new FileReader("data/2022-05_bmp180.csv"));
        String line = br.readLine(); //skip the header
        System.out.println("Header: " + line);
        while ((line = br.readLine()) != null) {
            String[] values = line.split(COMMA_DELIMITER);
            if (values.length == 10 && !values[5].isEmpty()) {
                var date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(values[5]);
                Timestamp timestamp = new Timestamp(date.getTime());
                Long sensor_id = Long.parseLong(values[0]);
                Double temperature = Double.parseDouble(values[9]);
                var message = String.format("%s;%d;%,.4f;", timestamp, sensor_id, temperature);
                var producerRecord = new ProducerRecord<>("flink-events", message);
                if (first) {
                    first = false;
                }
                else {
                    speed_factor = 5000000;
                    long diff = (timestamp.getTime() - previous.getTime())/speed_factor;
                    if (diff<0) diff = 0;
                    Thread.sleep(diff);
                }
                producer.send(producerRecord);
//                System.out.printf("Send: %s%n", message);
                previous = timestamp;
            }
        }
//        producer.send(new ProducerRecord<>("flink-events", String.format("%s;%d;%,.4f;", "2026-01-01 00:00:00", 0, 0.2)));
        br.close();
    }
}