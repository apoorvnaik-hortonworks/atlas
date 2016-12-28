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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.apache.atlas.metrics.AtlasMeter;
import org.apache.atlas.metrics.annotations.CollectMetric;
import org.apache.commons.lang.StringUtils;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class MetricsCollectionAspect extends AspectConfig {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsCollectionAspect.class);

    public MetricsCollectionAspect() {
        super();
    }

    /**
     * Collect required metrics for the intercepted method(s)
     *
     * @param proceedingJoinPoint Method interception point
     * @return Method invocation result
     * @throws Throwable
     */
    @Around("@annotation(org.apache.atlas.metrics.annotations.CollectMetric) && execution(* *(..))")
    public Object handleMetricsCollection(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        if (isEnabled) {
            MethodSignature methodSignature = (MethodSignature) proceedingJoinPoint.getSignature();
            /*
             * Identify the metering mode
             * 1. Meter all APIs
             * 2. Meter only mentioned in includes
             * 3. Meter all but the excludes
             */
            CollectMetric annotation = methodSignature.getMethod().getAnnotation(CollectMetric.class);
            if (annotation != null) {
                // Will be present as it's a required parameter
                String metricName = annotation.metricName();
                String meterName = StringUtils.isNotEmpty(annotation.meterName()) ? annotation.meterName() : deriveMeterName(methodSignature);

                // Only collect when enabled
                if (collectionEnabled(metricName, meterName)) {
                    long startTime = System.currentTimeMillis();
                    Object proceed = proceedingJoinPoint.proceed();

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Metering {}.{}", metricName, meterName);
                    }

                    MetricRegistry registry = SharedMetricRegistries.getOrCreate(metricName);
                    final AtlasMeter atlasMeter;
                    if (registry.getMetrics().containsKey(meterName)) {
                        atlasMeter = (AtlasMeter) registry.getMetrics().get(meterName);
                    } else {
                        atlasMeter = new AtlasMeter();
                        registry.register(meterName, atlasMeter);
                    }
                    atlasMeter.mark(System.currentTimeMillis() - startTime);

                    return proceed;
                }
            }
        }

        return proceedingJoinPoint.proceed();
    }

    private String deriveMeterName(MethodSignature methodSignature) {
        String className = methodSignature.getDeclaringType().getSimpleName();
        String methodName = methodSignature.getName();
        return StringUtils.isNotEmpty(className) ? className + "." + methodName : methodName;
    }
}
