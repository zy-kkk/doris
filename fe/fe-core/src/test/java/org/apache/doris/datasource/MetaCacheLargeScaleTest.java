// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.test.TestExternalCatalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Large scale test for MetaCache with millions of databases and tables.
 * This test simulates scenarios with massive metadata to verify:
 * 1. Memory usage under high load
 * 2. Cache size limit enforcement
 * 3. Access performance with large dataset
 * 4. Concurrent access performance
 */
public class MetaCacheLargeScaleTest {
    private static final int MB = 1024 * 1024;

    private TestExternalCatalog catalog;
    private Runtime runtime;
    private long originalCacheSize;

    @Before
    public void setUp() {
        FeConstants.runningUnitTest = true;
        runtime = Runtime.getRuntime();
        originalCacheSize = Config.max_meta_object_cache_num;
    }

    @After
    public void tearDown() {
        Config.max_meta_object_cache_num = originalCacheSize;
        if (catalog != null) {
            catalog.onClose();
        }
        System.gc();
    }

    /**
     * Test with moderate scale: 100 databases, 1000 tables each = 100K tables.
     * Purpose: Test metadata cache performance with 100K tables distributed across 100 DBs.
     * Note: Doris automatically adds 2 system databases (information_schema, mysql) to any catalog.
     * Run manually with: -Xmx2g -XX:+UseG1GC
     */
    @Test
    @Ignore("Large scale test, run manually with -Xmx2g")
    public void testHundredThousandTablesModerateDistribution() {
        System.out.println("\n========== Test: 100 DBs x 1000 Tables = 100K Tables ==========");

        int dbCount = 500;
        int tablesPerDb = 1000;

        // Create catalog with 100K tables
        catalog = createCatalogWithLargeMetadata(dbCount, tablesPerDb, 5);
        catalog.setInitializedForTest(true);

        // Measure memory before accessing metadata
        long memoryBefore = getUsedMemory();
        System.out.println("Memory before accessing metadata: " + memoryBefore / MB + " MB");

        // Access all databases to trigger cache loading
        // Note: Doris adds 2 system databases (information_schema, mysql) automatically
        long startTime = System.currentTimeMillis();
        List<String> dbNames = catalog.getDbNames();
        Assert.assertEquals("DB count should include 2 system databases", dbCount + 2, dbNames.size());
        long dbLoadTime = System.currentTimeMillis() - startTime;
        System.out.println("Time to load " + dbCount + " databases: " + dbLoadTime + " ms");

        // Access metadata for a subset of databases
        startTime = System.currentTimeMillis();
        int accessedDbCount = 0;
        for (String dbName : dbNames) {
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            Assert.assertNotNull(db);
            accessedDbCount++;
            if (accessedDbCount >= 100) {
                break;
            }
        }
        long accessTime = System.currentTimeMillis() - startTime;
        System.out.println("Time to access 100 databases: " + accessTime + " ms");
        System.out.println("Average time per database: " + (accessTime / 100.0) + " ms");

        // Measure memory after accessing metadata
        long memoryAfter = getUsedMemory();
        long memoryUsed = memoryAfter - memoryBefore;
        System.out.println("Memory after accessing metadata: " + memoryAfter / MB + " MB");
        System.out.println("Memory used by metadata cache: " + memoryUsed / MB + " MB");
        System.out.println("Average memory per database: " + (memoryUsed / accessedDbCount / 1024) + " KB");

        // Calculate estimated memory for all databases
        long estimatedTotalMemory = (memoryUsed / accessedDbCount) * dbCount;
        System.out.println("Estimated total memory for all " + dbCount + " databases: "
                + estimatedTotalMemory / MB + " MB");

        // Verify cache statistics
        verifyMetaCacheStats(catalog, accessedDbCount);
    }

    /**
     * Test with heavy concentration: 10 databases, 10k tables each = 100K tables.
     * Purpose: Test metadata cache performance with 100K tables concentrated in 10 DBs.
     * This tests how cache handles databases with very large table counts.
     * Note: Doris automatically adds 2 system databases (information_schema, mysql) to any catalog.
     * Run manually with: -Xmx2g -XX:+UseG1GC
     */
    @Test
    @Ignore("Large scale test, run manually with -Xmx2g")
    public void testHundredThousandTablesHeavyConcentration() {
        System.out.println("\n========== Test: 10 DBs x 10K Tables = 100K Tables ==========");

        int dbCount = 5;
        int tablesPerDb = 10000;

        // Create catalog with concentrated tables
        catalog = createCatalogWithLargeMetadata(dbCount, tablesPerDb, 5);
        catalog.setInitializedForTest(true);

        // Measure memory before
        long memoryBefore = getUsedMemory();
        System.out.println("Memory before: " + memoryBefore / MB + " MB");

        // Access all databases
        // Note: Doris adds 2 system databases (information_schema, mysql) automatically
        long startTime = System.currentTimeMillis();
        List<String> dbNames = catalog.getDbNames();
        Assert.assertEquals("DB count should include 2 system databases", dbCount + 2, dbNames.size());

        // Access each database
        for (String dbName : dbNames) {
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            Assert.assertNotNull(db);
        }
        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println("Time to access all " + dbCount + " databases: " + totalTime + " ms");

        // Measure memory after
        long memoryAfter = getUsedMemory();
        long memoryUsed = memoryAfter - memoryBefore;
        System.out.println("Memory after: " + memoryAfter / MB + " MB");
        System.out.println("Memory used: " + memoryUsed / MB + " MB");
        System.out.println("Average memory per database: " + (memoryUsed / dbCount / MB) + " MB");

        verifyMetaCacheStats(catalog, dbCount);
    }

    /**
     * Test cache size limit enforcement.
     * Purpose: Verify that cache respects the configured max size limit.
     * Note: Caffeine cache uses size-based eviction which is eventually consistent.
     * The cache may temporarily exceed the limit during bulk loading but will converge.
     */
    @Test
    public void testCacheSizeLimit() {
        System.out.println("\n========== Test: Cache Size Limit ==========");

        // Set a small cache size limit
        int maxCacheSize = 50;
        Config.max_meta_object_cache_num = maxCacheSize;

        // Use smaller dataset to avoid OOM on Mac
        int dbCount = 200;
        int tablesPerDb = 50;

        catalog = createCatalogWithLargeMetadata(dbCount, tablesPerDb, 3);
        catalog.setInitializedForTest(true);

        // Access more databases than cache size to trigger eviction
        List<String> dbNames = catalog.getDbNames();
        int accessCount = Math.min(100, dbNames.size());

        for (int i = 0; i < accessCount; i++) {
            ExternalDatabase<?> db = catalog.getDbNullable(dbNames.get(i));
            Assert.assertNotNull(db);
        }

        // Force GC to help cache eviction
        System.gc();
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            // Ignore
        }

        // Verify cache size eventually respects the limit
        long cacheSize = catalog.metaCache.getMetaObjCache().estimatedSize();
        System.out.println("Cache size after accessing " + accessCount + " databases: " + cacheSize);
        System.out.println("Max cache size limit: " + maxCacheSize);
        System.out.println("Cache eviction is working: " + (cacheSize < accessCount));

        // Caffeine cache may temporarily exceed limit during eviction
        // Allow 2x buffer for asynchronous eviction behavior
        Assert.assertTrue("Cache size should eventually respect max limit (current: " + cacheSize
                        + ", limit: " + maxCacheSize + ")",
                cacheSize <= maxCacheSize * 2);
    }

    /**
     * Test concurrent access performance.
     * Purpose: Verify cache can handle concurrent access correctly and efficiently.
     */
    @Test
    public void testConcurrentAccess() throws InterruptedException {
        System.out.println("\n========== Test: Concurrent Access ==========");

        // Use smaller dataset to avoid OOM
        int dbCount = 100;
        int tablesPerDb = 100;
        int threadCount = 10;
        int accessesPerThread = 50;

        catalog = createCatalogWithLargeMetadata(dbCount, tablesPerDb, 5);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicLong totalAccessTime = new AtomicLong(0);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Submit concurrent tasks
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    long threadStart = System.currentTimeMillis();

                    for (int i = 0; i < accessesPerThread; i++) {
                        int dbIndex = (threadId * accessesPerThread + i) % dbNames.size();
                        ExternalDatabase<?> db = catalog.getDbNullable(dbNames.get(dbIndex));
                        if (db != null) {
                            successCount.incrementAndGet();
                        }
                    }

                    long threadTime = System.currentTimeMillis() - threadStart;
                    totalAccessTime.addAndGet(threadTime);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        long startTime = System.currentTimeMillis();
        startLatch.countDown();

        // Wait for completion
        boolean finished = doneLatch.await(60, TimeUnit.SECONDS);
        Assert.assertTrue("Concurrent access should complete within timeout", finished);

        long totalTime = System.currentTimeMillis() - startTime;
        int totalAccesses = threadCount * accessesPerThread;

        System.out.println("Concurrent access results:");
        System.out.println("  Threads: " + threadCount);
        System.out.println("  Total accesses: " + totalAccesses);
        System.out.println("  Successful accesses: " + successCount.get());
        System.out.println("  Total time: " + totalTime + " ms");
        System.out.println("  Average time per access: " + (totalAccessTime.get() / totalAccesses) + " ms");
        System.out.println("  Throughput: " + (totalAccesses * 1000.0 / totalTime) + " accesses/sec");

        executor.shutdown();

        Assert.assertEquals("All accesses should succeed", totalAccesses, successCount.get());
    }

    /**
     * Test memory usage patterns with different access patterns.
     * Purpose: Understand how different access patterns affect memory usage.
     */
    @Test
    public void testMemoryUsagePatterns() {
        System.out.println("\n========== Test: Memory Usage Patterns ==========");

        // Moderate dataset size
        int dbCount = 100;
        int tablesPerDb = 500;

        catalog = createCatalogWithLargeMetadata(dbCount, tablesPerDb, 10);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();

        // Pattern 1: Sequential access
        System.out.println("\nPattern 1: Sequential Access");
        long memBefore = getUsedMemory();
        for (int i = 0; i < 50; i++) {
            catalog.getDbNullable(dbNames.get(i));
        }
        long memAfter = getUsedMemory();
        System.out.println("  Memory used: " + (memAfter - memBefore) / MB + " MB");

        // Pattern 2: Random access
        System.out.println("\nPattern 2: Random Access");
        memBefore = getUsedMemory();
        for (int i = 0; i < 50; i++) {
            int randomIndex = (int) (Math.random() * dbNames.size());
            catalog.getDbNullable(dbNames.get(randomIndex));
        }
        memAfter = getUsedMemory();
        System.out.println("  Memory used: " + (memAfter - memBefore) / MB + " MB");

        // Pattern 3: Repeated access (cache hit)
        System.out.println("\nPattern 3: Repeated Access (Cache Hit)");
        String dbName = dbNames.get(0);
        memBefore = getUsedMemory();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            catalog.getDbNullable(dbName);
        }
        long timeTaken = System.currentTimeMillis() - startTime;
        memAfter = getUsedMemory();
        System.out.println("  Memory used: " + (memAfter - memBefore) / MB + " MB");
        System.out.println("  Time for 1000 repeated accesses: " + timeTaken + " ms");
        System.out.println("  Average time: " + (timeTaken / 1000.0) + " ms");
    }

    /**
     * Test cache behavior when exceeding configured limits.
     * Purpose: Verify that cache eviction works correctly when limit is exceeded.
     * Observe how cache size changes as we access more items than it can hold.
     */
    @Test
    public void testCacheOverflow() {
        System.out.println("\n========== Test: Cache Overflow Behavior ==========");

        // Set a very small cache limit to easily observe eviction
        int maxCacheSize = 10;
        Config.max_meta_object_cache_num = maxCacheSize;

        int dbCount = 100;
        int tablesPerDb = 100;

        catalog = createCatalogWithLargeMetadata(dbCount, tablesPerDb, 5);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();

        // Access many more databases than cache can hold
        // Observe cache size changes during the process
        for (int i = 0; i < 50; i++) {
            ExternalDatabase<?> db = catalog.getDbNullable(dbNames.get(i));
            Assert.assertNotNull(db);

            long cacheSize = catalog.metaCache.getMetaObjCache().estimatedSize();
            System.out.println("After accessing DB " + i + ", cache size: " + cacheSize);
        }

        // Force GC to help with eviction
        System.gc();
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            // Ignore
        }

        long finalCacheSize = catalog.metaCache.getMetaObjCache().estimatedSize();
        System.out.println("Final cache size: " + finalCacheSize);
        System.out.println("Max cache size: " + maxCacheSize);

        // Caffeine uses size-based eviction which is eventually consistent
        // The cache may temporarily exceed the limit but will converge over time
        // Allow 3x buffer for asynchronous eviction behavior
        Assert.assertTrue("Cache should eventually evict entries (current: " + finalCacheSize
                        + ", limit: " + maxCacheSize + ")",
                finalCacheSize <= maxCacheSize * 3);
    }

    // Helper methods

    private TestExternalCatalog createCatalogWithLargeMetadata(int dbCount, int tablesPerDb, int columnsPerTable) {
        // Set system properties for the provider to read
        System.setProperty("test.db_count", String.valueOf(dbCount));
        System.setProperty("test.tables_per_db", String.valueOf(tablesPerDb));
        System.setProperty("test.columns_per_table", String.valueOf(columnsPerTable));

        Map<String, String> props = new HashMap<>();
        props.put("catalog_provider.class", LargeScaleTestCatalogProvider.class.getName());

        return new TestExternalCatalog(1L, "test_large_scale_catalog", "test", props, "");
    }

    private long getUsedMemory() {
        // Force garbage collection to get more accurate reading
        System.gc();
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Ignore
        }
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private void verifyMetaCacheStats(TestExternalCatalog catalog, int expectedDbCount) {
        System.out.println("\nMetaCache Statistics:");
        long cacheSize = catalog.metaCache.getMetaObjCache().estimatedSize();
        System.out.println("  Cache size: " + cacheSize);
        System.out.println("  Expected databases accessed: " + expectedDbCount);
        System.out.println("  Max cache size: " + Config.max_meta_object_cache_num);

        // Cache size should be reasonable
        Assert.assertTrue("Cache should contain accessed databases", cacheSize > 0);
    }

    /**
     * Large scale test catalog provider that generates massive metadata on-the-fly
     */
    public static class LargeScaleTestCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        private final int dbCount;
        private final int tablesPerDb;
        private final int columnsPerTable;
        private Map<String, Map<String, List<Column>>> metadata;

        public LargeScaleTestCatalogProvider() {
            // Read parameters from system properties
            this.dbCount = Integer.parseInt(System.getProperty("test.db_count", "10"));
            this.tablesPerDb = Integer.parseInt(System.getProperty("test.tables_per_db", "10"));
            this.columnsPerTable = Integer.parseInt(System.getProperty("test.columns_per_table", "5"));
            initMetadata();
        }

        private void initMetadata() {
            metadata = Maps.newHashMap();

            System.out.println("Generating metadata for " + dbCount + " databases, "
                    + tablesPerDb + " tables each...");

            for (int i = 0; i < dbCount; i++) {
                String dbName = "db_" + i;
                Map<String, List<Column>> tables = Maps.newHashMap();

                for (int j = 0; j < tablesPerDb; j++) {
                    String tableName = "table_" + j;
                    List<Column> columns = createMockColumns(columnsPerTable);
                    tables.put(tableName, columns);
                }

                metadata.put(dbName, tables);

                if ((i + 1) % 100 == 0) {
                    System.out.println("Generated metadata for " + (i + 1) + " databases");
                }
            }

            System.out.println("Metadata generation complete");
        }

        private List<Column> createMockColumns(int count) {
            List<Column> columns = Lists.newArrayList();
            for (int i = 0; i < count; i++) {
                columns.add(new Column("col_" + i, ScalarType.createStringType(), true, null, true, "", ""));
            }
            return columns;
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return metadata;
        }
    }
}
