package io.cdap.cdap.metrics.process.loader.dummy;

import com.google.gson.Gson;
import io.cdap.cdap.api.metrics.MetricValues;
import io.cdap.cdap.api.metrics.MetricsWriter;
import io.cdap.cdap.api.metrics.MetricsWriterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class DummyMetricsWriter implements MetricsWriter {
    private static final Logger LOG = LoggerFactory.getLogger(DummyMetricsWriter.class);
    private final static Gson GSON = new Gson();

    @Override
    public void write(Collection<MetricValues> metricValues) {
        metricValues.forEach(m -> LOG.info("Metrics received -> " + GSON.toJson(m)));
    }

    @Override
    public void initialize(MetricsWriterContext metricsWriterContext) {
        LOG.info("Initializing " + getID() + "...");
    }

    @Override
    public String getID() {
        return "DummyMetricsWriter";
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing " + getID() + "...");
    }
}
