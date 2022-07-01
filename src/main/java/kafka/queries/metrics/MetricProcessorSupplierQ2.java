package kafka.queries.metrics;

import kafka.queries.LocationAggregator;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import utils.tuples.ValQ1;

public class MetricProcessorSupplierQ2 implements ProcessorSupplier<String, LocationAggregator, Void, Void> {
    private String window;

    public MetricProcessorSupplierQ2(String window) {
        this.window = window;
    }

    @Override
    public Processor<String, LocationAggregator, Void, Void> get() {
        return new MetricProcessorQ2(window);
    }
}
