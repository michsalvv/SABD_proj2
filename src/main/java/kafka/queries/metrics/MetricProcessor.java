package kafka.queries.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import utils.tuples.ValQ1;

import java.util.HashMap;
import java.util.Map;

public class MetricProcessor implements Processor<Windowed<Long>, ValQ1, Void, Void> {
    private String window;

    public MetricProcessor(String window) {
        this.window = window;
    }

    public double recordCounter;
    public StreamsMetrics streamMetrics;
    public Sensor sensorThr;
    @Override
    public void process(Record record) {
        recordCounter++;
        sensorThr.record(1D);
    }

    @Override
    public void init(ProcessorContext context) {
        streamMetrics = context.metrics();
//        Sensor sensor = streamMetrics.addRateTotalSensor("test", "outProcessor", "thr", Sensor.RecordingLevel.INFO);
        Map<String, String> metricTags = new HashMap<String, String>();
        metricTags.put("metricTagKey", "metricsTagVal");

        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        Metrics metrics = new Metrics(metricConfig);

        sensorThr = metrics.sensor(window);
        MetricName metricName = metrics.metricName(window, window, window);
        sensorThr = streamMetrics.addSensor(window, Sensor.RecordingLevel.INFO);
        sensorThr.add(metricName, new CumulativeCount());

    }

    @Override
    public void close() {

        Processor.super.close();
    }
}
