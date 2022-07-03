package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import utils.exception.SimulationTimeException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import utils.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class Producer {

    private static String topic;

    public static void main(String[] args) throws InterruptedException, IOException, ParseException {

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-broker:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer<Long, String> producer = new KafkaProducer<>(props);

        switch (args[0]) {
            case ("flink"):
                topic = "flink-events";
                break;
            case ("kafka") :
                topic = "kafka-events";
                break;
        }

        boolean first = true;
        Timestamp previous = null;

        BufferedReader br = new BufferedReader(new FileReader(Config.ORIGINAL_DATASET));
        String line = br.readLine(); //skip the header

        while ((line = br.readLine()) != null) {
            String[] values = line.split(Config.COMMA_DELIMITER);
            if (values.length == 10 && !values[5].isEmpty()) {
                var date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(values[5]);

                DateTimeZone timeZone = DateTimeZone.forID( "Europe/Rome" );
                DateTime dateTime = new DateTime( date, timeZone );
                long ts = dateTime.getMillis();
                Timestamp timestamp = new Timestamp(ts);

                Long sensor_id = Long.parseLong(values[0]);
                Double temperature = Double.parseDouble(values[9]);
                Long location = Long.parseLong(values[2]);
                Double latitude = Double.parseDouble(values[3]);
                Double longitude = Double.parseDouble(values[4]);
                var message = String.format("%s;%d;%,.4f;%d;%,.4f;%,.4f;", timestamp, sensor_id, temperature,
                        location, latitude, longitude);
                var kafka_ts = ts + Config.CEST;
                var producerRecord = new ProducerRecord<>(topic, null, kafka_ts, sensor_id,  message);
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
                previous = timestamp;
            }
        }
        br.close();
    }

    static void validateTime(Timestamp actual, Timestamp previous) throws SimulationTimeException {
        long diff = (actual.getTime() - previous.getTime());
        if (diff < 0) {
            throw new SimulationTimeException("Producer Error: Timestamp out of Order");
        }
    }
}