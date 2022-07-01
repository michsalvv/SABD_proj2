package kafka.queries.metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class MetricsCalculator extends Thread {
    private Map<MetricName, ? extends Metric> metrics;
    private KafkaStreams stream;
    private long startTime;
    private double hourlyRecords, weeklyRecords, monthlyRecords, dailyRecords;
    private final FileWriter fileWriter;
    private String outputName;
    private String DELIMITER = ";";
    private StringBuilder outputBuilder;
    private String query;
    private int slot = 0;
    public MetricsCalculator(String filename, KafkaStreams stream, String query) {
        this.outputName = filename;
        this.stream = stream;
        this.query = query;

        try {
            Path path = Paths.get(filename);
            if (Files.exists(path)){
                Files.delete(path);
            }

            this.fileWriter = new FileWriter(outputName, true);
            this.outputBuilder = new StringBuilder("time;thr_hourly;thr_weekly;thr_monthly\n");
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

                if (query.contentEquals("Q1") && metricName.name().contentEquals("monthly-thr")) {
                    monthlyRecords = (double) metric.metricValue();
                }

                if (query.contentEquals("Q2") && metricName.name().contentEquals("daily-thr")) {
                    dailyRecords = (double) metric.metricValue();
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
        double monthlyTHR = 0;
        double dailyThr=0;

        if (query.contentEquals("Q1")) {
            monthlyTHR = monthlyRecords / ((currentTime - startTime) / 1000D);
        }else {
            dailyThr = dailyRecords / ((currentTime - startTime) / 1000D);
        }


        if (hourlyTHR > 0 || weeklyTHR > 0 || monthlyTHR > 0 || dailyThr > 0) {
            System.out.println("startTime: " + startTime);
            System.out.println("currentTime: " + currentTime);
            System.out.println("hourlyRec: " + this.hourlyRecords);
            System.out.println("weeklyRec: " + this.weeklyRecords);
            System.out.println("monthlyRec: " + this.monthlyRecords);
            System.out.println("hourlyTHR: " + hourlyTHR);
            System.out.println("weeklyTHR: " + weeklyTHR);
            System.out.println("monthlyTHR: " + monthlyTHR);
            System.out.println("dailyTHR: " + dailyThr);
            System.out.println("----------------\n");


            try {
                outputBuilder.append(slot).append(DELIMITER);
                outputBuilder.append(hourlyTHR).append(DELIMITER);
                outputBuilder.append(weeklyTHR).append(DELIMITER);
                if (query.contentEquals("Q1"))
                    outputBuilder.append(monthlyTHR).append("\n");
                else outputBuilder.append(dailyThr).append("\n");

                fileWriter.append(outputBuilder.toString());
                fileWriter.flush();
                outputBuilder.setLength(0);

                slot+=5;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
