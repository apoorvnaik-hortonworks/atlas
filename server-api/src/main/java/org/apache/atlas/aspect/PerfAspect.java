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
package org.apache.atlas.aspect;

import org.apache.atlas.RequestContext;
import org.apache.atlas.metrics.Metrics;
import org.apache.atlas.metrics.aspect.AspectConfig;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class PerfAspect extends AspectConfig {

    /**
     * Collect the number of invocations and total time spent for each invocation
     * @param joinPoint Monitored annotation cross-cut
     * @return Result of method invocation
     * @throws Throwable
     */
    @Around("@annotation(org.apache.atlas.metrics.annotations.Monitored) && execution(* *(..))")
    public Object collectPerformanceMetric(ProceedingJoinPoint joinPoint) throws Throwable {
        if (isEnabled) {
            Signature methodSign = joinPoint.getSignature();
            // Enable or Disable all in one go, no granularity
            if (collectionEnabled("RequestContext", "*")) {
                Metrics metrics = RequestContext.getMetrics();
                String metricName = methodSign.getDeclaringType().getSimpleName() + "." + methodSign.getName();
                long start = System.currentTimeMillis();

                try {
                    return joinPoint.proceed();
                } finally {
                    metrics.record(metricName, (System.currentTimeMillis() - start));
                }
            }
        }
        return joinPoint.proceed();
    }
}
