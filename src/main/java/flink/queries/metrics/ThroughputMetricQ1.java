package flink.queries.metrics;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import utils.tuples.OutputQuery;

public class ThroughputMetricQ1 extends RichMapFunction<OutputQuery, OutputQuery> {
        private transient Meter meter;

        @Override
        public void open(Configuration config) {
            com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();

            this.meter = getRuntimeContext()
                    .getMetricGroup()
                    .meter("Throughput", new DropwizardMeterWrapper(dropwizardMeter));
        }

        @Override
        public OutputQuery map(OutputQuery value) throws Exception {
            this.meter.markEvent();
            return value;
        }
}
