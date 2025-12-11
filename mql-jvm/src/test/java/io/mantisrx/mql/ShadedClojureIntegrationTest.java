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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
 * - Clojure + Java version compatibility (e.g., gvec.clj toArray issue)
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
    
    /**
     * This test verifies the shadow JAR doesn't have unshaded Clojure references
     * that would cause ClassNotFoundException at runtime.
     * 
     * It catches issues like:
     * - clojure.lang.Keyword (unshaded)
     * - Double-shading (io.mantisrx.mql.shaded.io.mantisrx.mql.shaded.clojure)
     */
    @Test
    public void testShadedJarNoUnshadedReferences() throws Exception {
        // Find the shadow JAR
        File buildLibsDir = new File("build/libs");
        File[] shadowJars = buildLibsDir.listFiles((dir, name) -> 
            name.startsWith("mql-jvm-") && name.endsWith(".jar") && !name.contains("sources") && !name.contains("javadoc"));
        
        if (shadowJars == null || shadowJars.length == 0) {
            // Skip if JAR not built yet
            System.out.println("Shadow JAR not found, skipping verification test");
            return;
        }
        
        File jarFile = shadowJars[0];
        System.out.println("Verifying shaded JAR: " + jarFile.getAbsolutePath());
        
        // Check for unshaded references in .clj files
        try (java.util.jar.JarFile jar = new java.util.jar.JarFile(jarFile)) {
            java.util.Enumeration<java.util.jar.JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                java.util.jar.JarEntry entry = entries.nextElement();
                String name = entry.getName();
                
                // Check .clj and .cljc source files for unshaded references
                if (name.endsWith(".clj") || name.endsWith(".cljc")) {
                    if (name.startsWith("io/mantisrx/mql/shaded/")) {
                        verifyNoUnshadedReferences(jar, entry, name);
                    }
                }
            }
        }
        
        System.out.println("✓ Shadow JAR verification passed - no unshaded references found!");
    }
    
    @SuppressWarnings("PMD.DataflowAnomalyAnalysis")
    private void verifyNoUnshadedReferences(java.util.jar.JarFile jar, 
                                             java.util.jar.JarEntry entry, 
                                             String name) throws Exception {
        java.util.regex.Pattern unshadedPattern = java.util.regex.Pattern.compile(
            "(?<!shaded\\.)clojure\\.(lang|core|asm|java|spec)\\.");
        
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(jar.getInputStream(entry)))) {
            java.util.List<String> lines = reader.lines().collect(java.util.stream.Collectors.toList());
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i);
                int lineNum = i + 1;
                // Skip comments
                if (line.trim().startsWith(";") || line.trim().startsWith(";;")) {
                    continue;
                }
                // Check for unshaded clojure references
                if (unshadedPattern.matcher(line).find()) {
                    fail("Unshaded Clojure reference in " + name + ":" + lineNum + " -> " + line);
                }
                // Check for double-shading
                if (line.contains("shaded.io.mantisrx.mql.shaded")) {
                    fail("Double-shaded reference in " + name + ":" + lineNum + " -> " + line);
                }
            }
        }
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

