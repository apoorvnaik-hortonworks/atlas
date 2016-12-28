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
package org.apache.atlas.metrics.service;

import com.google.inject.Inject;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.metrics.collector.MetricsCollector;
import org.apache.atlas.metrics.publisher.MetricsPublisher;
import org.apache.atlas.service.Service;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Metrics service acts like a daemon thread that captures atlas metrics
 * periodically and keep a track of the different counters, gauges and meters.
 *
 */
public class MetricsService implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

    private static final String ATLAS_METRICS_PREFIX = "atlas.metrics";
    private static final String METRICS_ENABLED = ATLAS_METRICS_PREFIX + ".enabled";
    private static final String COLLECTION_ENABLED = ATLAS_METRICS_PREFIX + ".collection.enabled";
    private static final String COLLECTION_INCLUDE = ATLAS_METRICS_PREFIX + ".collection.include";
    private static final String COLLECTION_EXCLUDE = ATLAS_METRICS_PREFIX + ".collection.exclude";
    private static final String PUBLISHING_ENABLED = ATLAS_METRICS_PREFIX + ".publishing.enabled";
    private static final String PUBLISHING_INCLUDE = ATLAS_METRICS_PREFIX + ".publishing.include";
    private static final String PUBLISHING_EXCLUDE = ATLAS_METRICS_PREFIX + ".publishing.exclude";
    private boolean metricsEnabled;
    private boolean collectEnabled;
    private String[] collectorInclude;
    private String[] collectorExclude;
    private boolean publishEnabled;
    private String[] publisherInclude;
    private String[] publisherExclude;

    private Set<MetricsCollector> metricsCollectors;
    private Set<MetricsPublisher> metricsPublishers;
    private ScheduledExecutorService fixedRateSchedulerService;

    @Inject
    public MetricsService(Set<MetricsCollector> metricsCollectors, Set<MetricsPublisher> metricsPublishers) {
        this.metricsCollectors = metricsCollectors;
        this.metricsPublishers = metricsPublishers;

        // Read all metrics config
        try {
            Configuration configuration = ApplicationProperties.get();
            metricsEnabled = configuration.getBoolean(METRICS_ENABLED, false);
            collectEnabled = configuration.getBoolean(COLLECTION_ENABLED, true);
            publishEnabled = configuration.getBoolean(PUBLISHING_ENABLED, true);
            collectorInclude = configuration.getStringArray(COLLECTION_INCLUDE);
            collectorExclude = configuration.getStringArray(COLLECTION_EXCLUDE);
            publisherInclude = configuration.getStringArray(PUBLISHING_INCLUDE);
            publisherExclude = configuration.getStringArray(PUBLISHING_EXCLUDE);
        } catch (AtlasException e) {
            LOG.error("Failed to configure MetricsService settings");
        } finally {
            // Create a threadPool with maximum needed threads
            fixedRateSchedulerService = Executors.newScheduledThreadPool(metricsCollectors.size() + metricsPublishers.size());
        }
    }

    @Override
    public void start() throws AtlasException {
        if (metricsEnabled) {
            if (CollectionUtils.isNotEmpty(metricsCollectors) && collectEnabled) {
                for (MetricsCollector metricsCollector : metricsCollectors) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Scheduling {} collection after fixed interval of {}",
                                metricsCollector.getClass().getSimpleName(),
                                metricsCollector.getCollectionIntervalInSecs());
                    }
                    String collectorName = metricsCollector.getClass().getSimpleName();
                    if (isEnabled(collectorInclude, collectorExclude, collectorName)) {
                        // Only allocate thread if not excluded or explicitly included
                        fixedRateSchedulerService.scheduleAtFixedRate(metricsCollector, 0,
                                metricsCollector.getCollectionIntervalInSecs(), TimeUnit.SECONDS);
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(metricsPublishers) && publishEnabled) {
                for (MetricsPublisher metricsPublisher : metricsPublishers) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Scheduling {} publish after fixed interval of {}",
                                metricsPublisher.getClass().getSimpleName(),
                                metricsPublisher.getPublishIntervalInSecs());
                    }
                    String publisherName = metricsPublisher.getClass().getSimpleName();
                    if (isEnabled(publisherInclude, publisherExclude, publisherName)) {
                        // Only allocate thread if not excluded or explicitly included
                        fixedRateSchedulerService.scheduleAtFixedRate(metricsPublisher, 0,
                                metricsPublisher.getPublishIntervalInSecs(), TimeUnit.SECONDS);
                    }
                }
            }
        }

    }

    @Override
    public void stop() throws AtlasException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Stopping MetricsService");
        }
        try {
            if (fixedRateSchedulerService != null) {
                fixedRateSchedulerService.shutdown();
                if (!fixedRateSchedulerService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    LOG.error("Timed out waiting for collector threads to shut down, exiting uncleanly");
                }
                fixedRateSchedulerService = null;
            }
        } catch (InterruptedException e) {
            LOG.error("Failure in shutting down consumers");
        }
    }

    private boolean isEnabled(String[] includes, String[] excludes, String name) {
        if (ArrayUtils.isNotEmpty(excludes) && ArrayUtils.contains(excludes, name)) {
            return false;
        } else {
            if (ArrayUtils.isNotEmpty(includes) && ArrayUtils.contains(includes, name)) {
                return true;
            }
        }
        return true;
    }
}
