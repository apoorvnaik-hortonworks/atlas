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
package org.apache.atlas.metrics.aspect;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AspectConfig {
    protected static Logger LOG = LoggerFactory.getLogger(AspectConfig.class);
    protected static boolean isEnabled = false;
    protected static String[] includes = null;
    protected static String[] excludes = null;

    public AspectConfig() {
        try {
            Configuration configuration = ApplicationProperties.get();
            isEnabled = configuration.getBoolean(AtlasConstants.ATLAS_METRICS_ENABLED, false);
            includes = configuration.getStringArray(AtlasConstants.ATLAS_METRICS_COLLECTION_INCLUDES);
            excludes = configuration.getStringArray(AtlasConstants.ATLAS_METRICS_COLLECTION_EXCLUDES);
        } catch (AtlasException e) {
            LOG.error("Unable to read Atlas config");
        }
        finally {
            if (excludes != null && excludes.length >= 1 && !excludes[0].equals("")) {
                excludes = null;
            }
            if (includes != null && includes.length >= 1 && !includes[0].equals("")) {
                includes = null;
            }
        }
    }

    private boolean hasNonEmptyExclusion() {
        return excludes != null;
    }

    private boolean hasNonEmptyInclusion() {
        return includes != null;
    }

    protected boolean collectionEnabled(String metricName, String meterName) {
        // If included or not excluded
        String qualifiedMetricName = metricName + "." + meterName;
        return hasNonEmptyInclusion() && ArrayUtils.contains(includes, qualifiedMetricName) ||
                hasNonEmptyExclusion() && !ArrayUtils.contains(excludes, qualifiedMetricName);
    }
}
