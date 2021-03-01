package com.azure.cosmos.kafka.connect.sink;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.implementation.BadRequestException;
import com.azure.cosmos.kafka.connect.CosmosDBConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the Kafka Task for the CosmosDB Sink Connector
 */
public class CosmosDBSinkTask extends SinkTask {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSinkTask.class);
    private CosmosClient client = null;
    private CosmosDBSinkConfig config;
    ObjectMapper mapper = new ObjectMapper();

    // Gather custom Cosmos metrics
    private Metrics cosmosMetrics;
    private Sensor cosmosRecordsSentSensor;
    private long cosmosMessagesSent = 0;

    @Override
    public String version() {
        return this.getClass().getPackage().getImplementationVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        logger.trace("Sink task started.");
        this.config = new CosmosDBSinkConfig(map);

        this.client = new CosmosClientBuilder()
                .endpoint(config.getConnEndpoint())
                .key(config.getConnKey())
                .userAgentSuffix(CosmosDBConfig.COSMOS_CLIENT_USER_AGENT_SUFFIX + version()).buildClient();

        client.createDatabaseIfNotExists(config.getDatabaseName());
        
        // Set up cosmos metrics
        setupCosmosMetrics();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (CollectionUtils.isEmpty(records)) {
            logger.info("No records to be written");
            return;
        }

        logger.info("Sending {} records to be written", records.size());

        Map<String, List<SinkRecord>> recordsByContainer = records.stream()
                // Find target container for each record
                .collect(Collectors.groupingBy(record -> config.getTopicContainerMap()
                        .getContainerForTopic(record.topic())
                        .orElseThrow(() -> new IllegalStateException(
                                String.format("No container defined for topic %s .", record.topic())))));

        for (Map.Entry<String, List<SinkRecord>> entry : recordsByContainer.entrySet()) {
            String containerName = entry.getKey();
            CosmosContainer container = client.getDatabase(config.getDatabaseName()).getContainer(containerName);
            for (SinkRecord record : entry.getValue()) {
                logger.debug("Writing record, value type: {}", record.value().getClass().getName());
                logger.debug("Key Schema: {}", record.keySchema());
                logger.debug("Value schema: {}", record.valueSchema());
                logger.debug("Value.toString(): {}", record.value() != null ? record.value().toString() : "<null>");

                Object recordValue;
                if (record.value() instanceof Struct) {
                    Map<String, Object> jsonMap = StructToJsonMap.toJsonMap((Struct) record.value());
                    recordValue = mapper.convertValue(jsonMap, JsonNode.class);
                } else {
                    recordValue = record.value();
                }

                try {
                    addItemToContainer(container, recordValue);
                    cosmosMessagesSent++;
                } catch (BadRequestException bre) {
                    throw new CosmosDBWriteException(record, bre);
                }
            }
        }

        cosmosRecordsSentSensor.record(cosmosMessagesSent);
        cosmosMessagesSent = 0;
    }

    private void addItemToContainer(CosmosContainer container, Object recordValue) {
        if (config.getUseUpsert().equalsIgnoreCase(CosmosDBConfig.BooleanValues.TRUE.toString())) {
            container.upsertItem(recordValue);
        } else {
            container.createItem(recordValue);
        }
    }

    private void setupCosmosMetrics() {
        logger.info("Setting up Sink Task custom metric handler.");

        JmxReporter jmxReporter = new JmxReporter();
        List<MetricsReporter> reporters = new ArrayList<>();
        reporters.add(jmxReporter);

        Map<String, String> metricTags = new LinkedHashMap<>();
        metricTags.put("connector", "cosmos-connector");
        metricTags.put("task", "sink-task-" + RandomUtils.nextLong(1L, 9999999L));
       
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        MetricsContext metricsContext = new KafkaMetricsContext("cosmos.kafka.connect", new LinkedHashMap<>());
        cosmosMetrics = new Metrics(metricConfig, reporters, Time.SYSTEM, metricsContext);
        cosmosRecordsSentSensor = cosmosMetrics.sensor("cosmos-records-sent");
        MetricName recordsSentRate = cosmosMetrics.metricName("cosmos-records-sent-rate", 
            "cosmos-task-metrics", "Cosmos records sent rate");
        MetricName recordsSentTotal = cosmosMetrics.metricName("cosmos-records-sent-total",
            "cosmos-task-metrics", "Cosmos records sent total");
        cosmosRecordsSentSensor.add(recordsSentRate, new Rate());
        cosmosRecordsSentSensor.add(recordsSentTotal, new CumulativeSum());
    }

    @Override
    public void stop() {
        logger.trace("Stopping CosmosDB sink task");

        if (client != null) {
            client.close();
            client = null;
        }

        client = null;
        cosmosMetrics.close();
    }
}
