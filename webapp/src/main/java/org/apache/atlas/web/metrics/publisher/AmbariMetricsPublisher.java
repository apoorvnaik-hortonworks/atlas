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
package org.apache.atlas.web.metrics.publisher;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.metrics.publisher.MetricsPublisher;
import org.apache.atlas.metrics.publisher.Publisher;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.AtlasConstants.ATLAS_METRICS_PUBLISH_INTERVAL;

public class AmbariMetricsPublisher implements MetricsPublisher {
    private final static Logger LOG = LoggerFactory.getLogger(AmbariMetricsPublisher.class);
    private static final String PUBLISHER_NAME = "atlas.ambari.metrics.publisher";
    private static int defaultIntervalInSecs = 15;

    private Publisher ambariPublisher;

    public AmbariMetricsPublisher() {
        try {
            Configuration configuration = ApplicationProperties.get();
            defaultIntervalInSecs = configuration.getInt(ATLAS_METRICS_PUBLISH_INTERVAL, defaultIntervalInSecs);
            Class ambariMetricPublisher = ApplicationProperties.getClass(configuration, PUBLISHER_NAME, null, Publisher.class);
            ambariPublisher = (Publisher) ambariMetricPublisher.newInstance();
        } catch (AtlasException | IllegalAccessException | InstantiationException e) {
            LOG.error("Init failed for AmbariMetricsPublisher. Reason : {}", e.getMessage());
        }
    }

    @Override
    public int getPublishIntervalInSecs() {
        return defaultIntervalInSecs;
    }

    @Override
    public void run() {
        // Check if ambari publisher is available for publishing metrics
        if (ambariPublisher != null) {
            Set<String> names = SharedMetricRegistries.names();
            List<MetricRegistry> currentMetrics = new ArrayList<>();
            for (String name : names) {
                currentMetrics.add(SharedMetricRegistries.getOrCreate(name));
            }
            if (currentMetrics.size() > 0) {
                // Only publish if there are some metrics
                publish(currentMetrics);
            }
        }
    }

    @Override
    public void publish(List<MetricRegistry> metrics) {
        if (ambariPublisher != null) {
            LOG.info("Publishing {} metrics.", metrics.size());
            ambariPublisher.publish(metrics);
        }
    }
}
