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

import com.codahale.metrics.SharedMetricRegistries;
import org.apache.atlas.AtlasException;
import org.apache.atlas.metrics.publisher.Publisher;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AmbariMetricsPublisherTest {

    @Mock
    Publisher mockPublisher;

    @InjectMocks
    AmbariMetricsPublisher testPublisher = new AmbariMetricsPublisher();


    @BeforeClass
    public void init() throws AtlasException {
        MockitoAnnotations.initMocks(this);
        testPublisher = spy(testPublisher);
    }

    @Test
    public void testGoodConfig() {
        // Create a test metric repo
        SharedMetricRegistries.getOrCreate("TestRegistry");
        testPublisher.run();
        verify(mockPublisher).publish(anyList());

        SharedMetricRegistries.remove("TestRegistry");
        testPublisher.run();
        verifyNoMoreInteractions(mockPublisher);
    }

    @Test(dependsOnMethods = "testGoodConfig")
    public void testAmbariPublisherNotConfigured(){
        testPublisher = spy(new AmbariMetricsPublisher());
        testPublisher.run();
        verify(testPublisher, never()).publish(anyList());
    }

}