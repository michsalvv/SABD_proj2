package kafka.queries.metrics;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import scala.Enumeration;
import utils.tuples.ValQ1;

public class MetricProcessorSupplier implements ProcessorSupplier<Windowed<Long>, ValQ1, Void, Void> {
    private String window;

    public MetricProcessorSupplier(String window) {
        this.window = window;
    }

    @Override
    public Processor<Windowed<Long>, ValQ1, Void, Void> get() {
        return new MetricProcessor(window);
    }
}
