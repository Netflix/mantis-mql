/*
 * Copyright 2022 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mantisrx.mql;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import io.mantisrx.mql.jvm.core.Query;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that verifies the MQL library works correctly via the Java/Clojure interop API.
 * This simulates how applications use the MQL library.
 * 
 * Note: During testing, we use the unshaded Clojure API (clojure.java.api.Clojure).
 * In the shaded JAR, these become io.mantisrx.mql.shaded.clojure.java.api.Clojure.
 * 
 * This test helps catch issues like:
 * - Missing Clojure core functions
 * - Parser (instaparse) problems
 * - Namespace loading issues
 */
public class ShadedClojureIntegrationTest {

    private static IFn require;
    private static IFn makeQuery;

    @BeforeClass
    public static void setUp() {
        // This mirrors how applications typically initialize MQL
        require = Clojure.var("clojure.core", "require");
        
        // Require the MQL namespaces
        require.invoke(Clojure.read("io.mantisrx.mql.jvm.interfaces.server"));
        require.invoke(Clojure.read("io.mantisrx.mql.jvm.interfaces.core"));
        
        // Get the make-query function
        makeQuery = Clojure.var("io.mantisrx.mql.jvm.interfaces.server", "make-query");
    }

    @Test
    public void testCreateSimpleQuery() {
        // Create a simple query
        Query query = (Query) makeQuery.invoke("test-subscription", "SELECT * WHERE true");
        
        assertThat(query).isNotNull();
        assertThat(query.getSubscriptionId()).isEqualTo("test-subscription");
        assertThat(query.getRawQuery()).isEqualTo("SELECT * WHERE true");
    }

    @Test
    public void testQueryMatches() {
        Query query = (Query) makeQuery.invoke("test-sub", "SELECT * WHERE x > 5");
        
        Map<String, Object> matchingData = new HashMap<>();
        matchingData.put("x", 10);
        
        Map<String, Object> nonMatchingData = new HashMap<>();
        nonMatchingData.put("x", 3);
        
        assertThat(query.matches(matchingData)).isTrue();
        assertThat(query.matches(nonMatchingData)).isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testQueryProjection() {
        Query query = (Query) makeQuery.invoke("test-sub", "SELECT x, y WHERE true");
        
        Map<String, Object> data = new HashMap<>();
        data.put("x", 1);
        data.put("y", 2);
        data.put("z", 3);  // Should be excluded from projection
        
        Map<String, Object> projected = query.project(data);
        
        assertThat(projected).containsKeys("x", "y");
    }

    @Test
    public void testQueryWithComplexExpression() {
        // Test a more complex query to ensure the instaparse parser works
        Query query = (Query) makeQuery.invoke("complex-test", 
            "SELECT name, value WHERE category == 'metrics' AND value > 100");
        
        assertThat(query).isNotNull();
        assertThat(query.getSubscriptionId()).isEqualTo("complex-test");
        
        Map<String, Object> matchingData = new HashMap<>();
        matchingData.put("name", "cpu");
        matchingData.put("value", 150);
        matchingData.put("category", "metrics");
        
        assertThat(query.matches(matchingData)).isTrue();
    }
}

