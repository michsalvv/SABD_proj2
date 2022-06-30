package kafka.queries.metrics;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.streams.KafkaStreams;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class MetricsCalculator extends Thread {
    private Map<MetricName, ? extends Metric> metrics;
    private KafkaStreams stream;
    private long startTime;
    private double hourlyRecords, weeklyRecords, monthlyRecords;
    private FileWriter fileWriter;
    String outputName = "Results/kafka_thr_query1.csv";
    String DELIMITER = ";";
    StringBuilder outputBuilder;

    public void start(KafkaStreams stream)  {
        try {
            this.fileWriter = new FileWriter(outputName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.outputBuilder = new StringBuilder("thr_hourly;thr_weekly;thr_monthly\n");
        this.stream = stream;
        run();
    }

    @Override
    public void run() {

        while (true) {
            metrics = stream.metrics();

            metrics.forEach((metricName, metric) -> {
                if (startTime == 0L && metricName.name().contentEquals("start-time-ms")) {
//                    System.out.println(metricName +"  = " + metric.metricValue());
                    startTime = (long) metric.metricValue();
                }

                if (metricName.name().contentEquals("hourly-thr")) {
//                    System.out.println(metricName.name() + "  " + metricName.group() + "  = " + metric.metricValue());
                    hourlyRecords = (double) metric.metricValue();
                }

                if (metricName.name().contentEquals("weekly-thr")) {
//                    System.out.println(metricName.name() + "  " + metricName.group() + "  = " + metric.metricValue());
                    weeklyRecords = (double) metric.metricValue();
                }

                if (metricName.name().contentEquals("monthly-thr")) {
//                    System.out.println(metricName.name() + "  " + metricName.group() + "  = " + metric.metricValue());
                    monthlyRecords = (double) metric.metricValue();
                }

                calculateTHR();

            });
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

//        if (hourlyTHR>0 || weeklyTHR>0 || monthlyTHR>0) {
        System.out.println("startTime: " + startTime);
        System.out.println("currentTime: " + currentTime);
        System.out.println("hourlyRec: " + this.hourlyRecords);
        System.out.println("weeklyRec: " + this.weeklyRecords);
        System.out.println("monthlyRec: " + this.monthlyRecords);
        System.out.println("hourlyTHR: " + hourlyTHR);
        System.out.println("weeklyTHR: " + weeklyTHR);
        System.out.println("monthlyTHR: " + monthlyTHR);
        System.out.println("----------------\n");


        try {
            outputBuilder.append(hourlyTHR).append(DELIMITER);
            outputBuilder.append(weeklyTHR).append(DELIMITER);
            outputBuilder.append(monthlyTHR).append(DELIMITER).append("\n");
            fileWriter.append(outputBuilder.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


//        }
    }
}
