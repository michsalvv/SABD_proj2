package kafka.queries.metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class MetricsCalculator extends Thread {
    private Map<MetricName, ? extends Metric> metrics;
    private KafkaStreams stream;
    private long startTime;
    private double hourlyRecords, weeklyRecords, monthlyRecords;
    private final FileWriter fileWriter;
    String outputName;
    String DELIMITER = ";";
    StringBuilder outputBuilder;

    public MetricsCalculator(String filename, KafkaStreams stream) {
        this.outputName = filename;
        this.stream = stream;

        try {
            Files.delete(Paths.get(filename));

            this.fileWriter = new FileWriter(outputName, true);
            this.outputBuilder = new StringBuilder("thr_hourly;thr_weekly;thr_monthly\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public void run() {

        while (true) {
            metrics = stream.metrics();

            metrics.forEach((metricName, metric) -> {
                if (startTime == 0L && metricName.name().contentEquals("start-time-ms")) {
                    startTime = (long) metric.metricValue();
                }

                if (metricName.name().contentEquals("hourly-thr")) {
                    hourlyRecords = (double) metric.metricValue();
                }

                if (metricName.name().contentEquals("weekly-thr")) {
                    weeklyRecords = (double) metric.metricValue();
                }

                if (metricName.name().contentEquals("monthly-thr")) {
                    monthlyRecords = (double) metric.metricValue();
                }
            });

            calculateTHR();
            try {
                sleep(5 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void calculateTHR() {
        long currentTime = System.currentTimeMillis();
        double hourlyTHR = hourlyRecords / ((currentTime - startTime) / 1000D);
        double weeklyTHR = weeklyRecords / ((currentTime - startTime) / 1000D);
        double monthlyTHR = monthlyRecords / ((currentTime - startTime) / 1000D);

        if (hourlyTHR > 0 || weeklyTHR > 0 || monthlyTHR > 0) {
//            System.out.println("startTime: " + startTime);
//            System.out.println("currentTime: " + currentTime);
//            System.out.println("hourlyRec: " + this.hourlyRecords);
//            System.out.println("weeklyRec: " + this.weeklyRecords);
//            System.out.println("monthlyRec: " + this.monthlyRecords);
//            System.out.println("hourlyTHR: " + hourlyTHR);
//            System.out.println("weeklyTHR: " + weeklyTHR);
//            System.out.println("monthlyTHR: " + monthlyTHR);
//            System.out.println("----------------\n");


            try {
                outputBuilder.append(hourlyTHR).append(DELIMITER);
                outputBuilder.append(weeklyTHR).append(DELIMITER);
                outputBuilder.append(monthlyTHR).append("\n");

                fileWriter.append(outputBuilder.toString());
                outputBuilder.setLength(0);
                fileWriter.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
