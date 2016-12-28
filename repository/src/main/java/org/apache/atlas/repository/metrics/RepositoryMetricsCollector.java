/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasException;
import org.apache.atlas.metrics.collector.MetricsCollector;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepositoryMetricsCollector implements MetricsCollector {
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryMetricsCollector.class);

    private static final String METRIC_REGISTY_NAME = "RepositoryMetrics";

    private static final String TYPE_METRIC_PREFIX = "Type.";
    private static final String METRIC_TYPE_COUNT = TYPE_METRIC_PREFIX + "count";
    private static final String METRIC_TYPE_ENTITIES = TYPE_METRIC_PREFIX + "%s.entities";

    private static final String ENTITY_METRIC_PREFIX = "Entity.";
    private static final String METRIC_ENTITY_COUNT = ENTITY_METRIC_PREFIX + "count";
    private static final String METRIC_TAGGED_ENTITIES = ENTITY_METRIC_PREFIX + "tags";
    private static final String METRIC_TAGGED_ENTITIES_MIN = METRIC_TAGGED_ENTITIES + ".min";
    private static final String METRIC_TAGGED_ENTITIES_MAX = METRIC_TAGGED_ENTITIES + ".max";
    private static final String METRIC_TAGGED_ENTITIES_AVG = METRIC_TAGGED_ENTITIES + ".avg";

    private static final String TAG_METRIC_PREFIX = "Tag.";
    private static final String METRIC_TAG_COUNT = TAG_METRIC_PREFIX + "count";
    private static final String METRIC_TAG_ENTITIES = TAG_METRIC_PREFIX + "%s.entities";

    private final MetricRegistry metricRegistry;
    private final AtlasGraph atlasGraph;

    private static int defaultIntervalInSecs = 15;

    public RepositoryMetricsCollector() {
        atlasGraph = AtlasGraphProvider.getGraphInstance();
        metricRegistry = SharedMetricRegistries.getOrCreate(METRIC_REGISTY_NAME);
        try {
            Configuration configuration = ApplicationProperties.get();
            defaultIntervalInSecs = configuration.getInt(AtlasConstants.ATLAS_METRICS_COLLECTION_INTERVAL, defaultIntervalInSecs);
        } catch (AtlasException e) {
            LOG.error("Atlas config read failed for collector config. Using default value : 15 Mins");
        }
    }


    @Override
    public int getCollectionIntervalInSecs() {
        return defaultIntervalInSecs;
    }

    @Override
    public void run() {
        LOG.info("Collecting GAUGE metrics");

        // TODO: Fire gremlin queries to capture required metrics
        for (MetricQueries query : MetricQueries.values()) {
            // TODO: execute metric queries and update gauge with actual values
            updateGauge(query.getMetricName(), 5);
        }
    }

    private void updateGauge(String metric, final Integer value) {
        if (metricRegistry.getGauges().get(metric) != null) {
            metricRegistry.remove(metric);
        }
        metricRegistry.register(metric, new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return value;
            }
        });
    }

    enum MetricQueries {
        // TODO: Complete the missing queries
        COUNT_TYPES(METRIC_TYPE_COUNT, "g.V().has('__type', 'typeSystem').count()"),
        COUNT_ENTITIES(METRIC_ENTITY_COUNT, "g.V().has('__superTypeNames', 'Referenceable').count()"),
        ENTITIES_PER_TYPE(METRIC_TYPE_ENTITIES, ""),
        TAGGED_ENTITIES(METRIC_TAGGED_ENTITIES, "g.V().has('__superTypeNames', 'Referenceable').has('__traitNames').count()"),
        TAGGED_ENTITIES_MIN(METRIC_TAGGED_ENTITIES_MIN, ""),
        TAGGED_ENTITIES_MAX(METRIC_TAGGED_ENTITIES_MAX, ""),
        TAGGED_ENTITIES_AVG(METRIC_TAGGED_ENTITIES_AVG, ""),
        COUNT_TAGS(METRIC_TAG_COUNT, ""),
        ENTITIES_PER_TAG(METRIC_TAG_ENTITIES, "")
        ;
        private String gremlinQuery;
        private String metric;

        MetricQueries(String metric, String gremlinQuery) {
            this.metric = metric;
            this.gremlinQuery = gremlinQuery;
        }

        public String getGremlinQuery() {
            return gremlinQuery;
        }

        public String getMetricName() { return metric; }

        @Override
        public String toString() {
            return "MetricQueries{" + "gremlinQuery='" + gremlinQuery + '\'' + ", metric='" + metric + '\'' + '}';
        }
    }
}
