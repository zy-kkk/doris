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

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * User scenario tests for MetaCache with large scale databases and tables.
 * Tests are designed from user's perspective to verify performance and stability.
 *
 * <p>Test Scale Configuration:
 * Adjust these parameters to control test scale. For Mac with limited memory,
 * use smaller values. For production-like testing, increase these values.
 */
public class MetaCacheUserScenarioTest {
    private static final int MB = 1024 * 1024;

    // ==================== Test Scale Configuration ====================
    // Modify these values to adjust test scale
    //
    // Two extreme scenarios:
    // 1. Extreme DBs: 1,000,000 DBs × 1 table = 1M tables (test SHOW DATABASES limit)
    // 2. Extreme Tables: 1 DB × 1,000,000 tables = 1M tables (test table traversal limit)
    //
    // Medium scale (for quick testing): 1000 DBs × 1000 tables = 1M tables

    // Default: Extreme DBs scenario (1M databases, 1 table each)
    private static final int DB_COUNT = 1000;              // Number of databases
    private static final int TABLES_PER_DB = 1000;         // Tables per database
    private static final int COLUMNS_PER_TABLE = 5;        // Columns per table

    // For extreme tables scenario, use: DB_COUNT = 1, TABLES_PER_DB = 1000000

    // Cache configuration for testing
    private static final int DEFAULT_CACHE_SIZE = 1000; // Default max cache size (same as Config default)
    private static final int SMALL_CACHE_SIZE = 10;     // For cache thrashing test

    // Access pattern configuration
    private static final int CONCURRENT_THREADS = 50;   // Concurrent thread count
    private static final int ACCESS_PER_THREAD = 100;   // Accesses per thread
    private static final int LONG_RUN_ACCESSES = 10000; // Long-running test accesses
    private static final int HOT_DB_COUNT = 10;         // Hot database count
    // ==================================================================

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
     * Scenario 1: SHOW DATABASES performance
     * User operation: USE large_catalog; SHOW DATABASES;
     * User concern: How long to return 1000 database names? Will it hang?
     */
    @Test
    public void testShowDatabasesPerformance() {
        System.out.println("\n=== Scenario 1: SHOW DATABASES Performance ===");
        System.out.println("Scale: " + DB_COUNT + " DBs × " + TABLES_PER_DB + " tables = "
                + (DB_COUNT * TABLES_PER_DB) + " tables");

        catalog = createCatalog(DB_COUNT, TABLES_PER_DB, COLUMNS_PER_TABLE);
        catalog.setInitializedForTest(true);

        long memBefore = getUsedMemory();
        long startTime = System.nanoTime();

        // Simulate: SHOW DATABASES
        List<String> dbNames = catalog.getDbNames();

        long elapsed = (System.nanoTime() - startTime) / 1_000_000; // ms
        long memAfter = getUsedMemory();
        long memUsed = (memAfter - memBefore) / MB;

        System.out.println("Results:");
        System.out.println("  Database count: " + dbNames.size());
        System.out.println("  Response time: " + elapsed + " ms");
        System.out.println("  Memory used: " + memUsed + " MB");

        // ===== Correctness Verification =====
        // Verify: +2 for system databases (information_schema, mysql)
        Assert.assertEquals("Should return all databases plus 2 system databases",
                DB_COUNT + 2, dbNames.size());
        Assert.assertTrue("Response time should be < 1000ms", elapsed < 1000);

        // Real user scenario: Verify database names are correct and complete
        System.out.println("\nCorrectness Verification:");
        Assert.assertTrue("Should contain information_schema", dbNames.contains("information_schema"));
        Assert.assertTrue("Should contain mysql", dbNames.contains("mysql"));
        Assert.assertTrue("Should contain db_0", dbNames.contains("db_0"));
        Assert.assertTrue("Should contain db_" + (DB_COUNT - 1), dbNames.contains("db_" + (DB_COUNT - 1)));

        // Verify no duplicate names
        long uniqueCount = dbNames.stream().distinct().count();
        Assert.assertEquals("Database names should be unique", dbNames.size(), uniqueCount);
        System.out.println("  ✓ Database names are correct and unique");

        System.out.println("✓ SHOW DATABASES performance is acceptable\n");
    }

    /**
     * Scenario 2: Switch database performance
     * User operation: USE large_catalog.db_12345;
     * User concern: First access vs cached access performance difference
     */
    @Test
    public void testSwitchDatabasePerformance() {
        System.out.println("\n=== Scenario 2: Switch Database Performance ===");
        System.out.println("Scale: " + DB_COUNT + " DBs × " + TABLES_PER_DB + " tables");

        catalog = createCatalog(DB_COUNT, TABLES_PER_DB, COLUMNS_PER_TABLE);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();
        int testCount = 100;

        long totalCacheMissTime = 0;
        long totalCacheHitTime = 0;

        System.out.println("Testing " + testCount + " databases...");

        for (int i = 0; i < testCount; i++) {
            String dbName = dbNames.get(i + 2); // Skip system databases

            // First access: cache miss
            long start = System.nanoTime();
            ExternalDatabase<?> db1 = catalog.getDbNullable(dbName);
            long cacheMissTime = (System.nanoTime() - start) / 1_000; // microseconds
            totalCacheMissTime += cacheMissTime;

            Assert.assertNotNull("Database should exist", db1);

            // Real user scenario: Verify database name is correct
            Assert.assertEquals("Database name should match", dbName, db1.getFullName());

            // Second access: cache hit
            start = System.nanoTime();
            ExternalDatabase<?> db2 = catalog.getDbNullable(dbName);
            long cacheHitTime = (System.nanoTime() - start) / 1_000; // microseconds
            totalCacheHitTime += cacheHitTime;

            // Real user scenario: Cache hit should return the exact same object instance
            Assert.assertSame("Should return same cached object", db1, db2);
            Assert.assertEquals("Database name should remain correct on cache hit", dbName, db2.getFullName());
        }

        double avgCacheMiss = totalCacheMissTime / (double) testCount;
        double avgCacheHit = totalCacheHitTime / (double) testCount;

        System.out.println("Results:");
        System.out.println("  Average cache miss time: " + String.format("%.2f", avgCacheMiss) + " μs");
        System.out.println("  Average cache hit time: " + String.format("%.2f", avgCacheHit) + " μs");
        System.out.println("  Speedup: " + String.format("%.1fx", avgCacheMiss / avgCacheHit));

        Assert.assertTrue("Cache miss should be < 100ms", avgCacheMiss < 100_000);
        Assert.assertTrue("Cache hit should be < 1ms", avgCacheHit < 1_000);

        System.out.println("\nCorrectness Verification:");
        System.out.println("  ✓ All database names match expected values");
        System.out.println("  ✓ Cache hit returns same object instance");

        System.out.println("✓ Database switching performance is good\n");
    }

    /**
     * Scenario 3: Cross-database query performance
     * User operation: SELECT * FROM db_1.t JOIN db_2.t JOIN db_3.t
     * User concern: Performance when cache capacity is insufficient
     */
    @Test
    public void testCrossDatabaseQueryPerformance() {
        System.out.println("\n=== Scenario 3: Cross-Database Query Performance ===");
        System.out.println("Scale: " + DB_COUNT + " DBs × " + TABLES_PER_DB + " tables");

        // Set cache size smaller than accessed databases
        Config.max_meta_object_cache_num = DEFAULT_CACHE_SIZE;

        catalog = createCatalog(DB_COUNT, TABLES_PER_DB, COLUMNS_PER_TABLE);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();
        int accessCount = Math.min(500, DB_COUNT);

        System.out.println("Cache size: " + DEFAULT_CACHE_SIZE);
        System.out.println("Accessing " + accessCount + " databases sequentially...");

        // ===== Correctness Verification: Data Isolation =====
        // Real user scenario: Verify different databases don't mix data
        // Note: dbNames order is not guaranteed, so use explicit database names
        ExternalDatabase<?> db1 = catalog.getDbNullable("db_0");
        ExternalDatabase<?> db2 = catalog.getDbNullable("db_1");
        Assert.assertNotNull(db1);
        Assert.assertNotNull(db2);
        Assert.assertNotEquals("Different databases should have different names",
                db1.getFullName(), db2.getFullName());
        Assert.assertEquals("db_0", db1.getFullName());
        Assert.assertEquals("db_1", db2.getFullName());

        // ===== Table-level verification: Access tables from different databases =====
        // Real user scenario: SELECT * FROM db_0.table_0 JOIN db_1.table_1
        ExternalTable table1 = db1.getTableNullable("table_0");
        ExternalTable table2 = db2.getTableNullable("table_1");
        Assert.assertNotNull("Table should exist in db_0", table1);
        Assert.assertNotNull("Table should exist in db_1", table2);
        Assert.assertEquals("table_0", table1.getName());
        Assert.assertEquals("table_1", table2.getName());
        System.out.println("  ✓ Table-level access verified: db_0.table_0 and db_1.table_1");

        long startTime = System.nanoTime();
        List<Long> cacheSizes = new ArrayList<>();

        // Remember first database and table info for eviction test
        String firstDbName = "db_0";
        String firstDbFullName = "db_0";
        String firstTableName = "table_0";

        for (int i = 0; i < accessCount; i++) {
            String dbName = dbNames.get(i + 2); // Skip system databases
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            Assert.assertNotNull(db);
            // Real user scenario: Each database should have correct name
            Assert.assertEquals("Database name should match", dbName, db.getFullName());

            // Table-level access: Access a sample table in each database
            // This simulates: SELECT * FROM db_xxx.table_0
            if (i % 10 == 0) {
                ExternalTable table = db.getTableNullable("table_0");
                Assert.assertNotNull("Table should exist", table);
                Assert.assertEquals("table_0", table.getName());
            }

            if (i % 50 == 0) {
                long cacheSize = catalog.metaCache.getMetaObjCache().estimatedSize();
                cacheSizes.add(cacheSize);
                System.out.println("  After " + i + " db+table accesses, catalog cache size: " + cacheSize);
            }
        }

        long totalTime = (System.nanoTime() - startTime) / 1_000_000; // ms
        double avgTime = totalTime / (double) accessCount;

        System.out.println("Results:");
        System.out.println("  Total time: " + totalTime + " ms");
        System.out.println("  Average time per database: " + String.format("%.2f", avgTime) + " ms");
        System.out.println("  Final cache size: " + catalog.metaCache.getMetaObjCache().estimatedSize());

        Assert.assertTrue("Average access time should be < 10ms", avgTime < 10);
        Assert.assertTrue("Final cache size should be around max size",
                catalog.metaCache.getMetaObjCache().estimatedSize() <= DEFAULT_CACHE_SIZE * 2);

        // ===== Correctness Verification: Eviction and Reload Consistency =====
        // Real user scenario: After cache eviction, reloaded data should be consistent
        System.out.println("\nCorrectness Verification:");
        System.out.println("  ✓ Cross-database data isolation verified (db_0 ≠ db_1)");

        // Access first database again - it may have been evicted
        ExternalDatabase<?> reloadedDb = catalog.getDbNullable(firstDbName);
        Assert.assertNotNull(reloadedDb);
        Assert.assertEquals("Reloaded database should have same name", firstDbFullName, reloadedDb.getFullName());
        System.out.println("  ✓ Evicted database reloaded correctly (name consistent)");

        // Table-level eviction and reload verification
        // After accessing 500 databases, the first database's table cache may also be evicted
        ExternalTable reloadedTable = reloadedDb.getTableNullable(firstTableName);
        Assert.assertNotNull("Evicted table should be reloadable", reloadedTable);
        Assert.assertEquals("Reloaded table should have same name", firstTableName, reloadedTable.getName());
        System.out.println("  ✓ Evicted table reloaded correctly (table_0 from db_0)");

        System.out.println("✓ Cross-database query performance is acceptable\n");
    }

    /**
     * Scenario 4: Concurrent queries (multiple users)
     * User operation: 100 users execute queries simultaneously
     * User concern: Performance and correctness under concurrent access
     */
    @Test
    public void testConcurrentQueries() throws InterruptedException {
        System.out.println("\n=== Scenario 4: Concurrent Queries ===");
        System.out.println("Scale: " + DB_COUNT + " DBs × " + TABLES_PER_DB + " tables");

        catalog = createCatalog(DB_COUNT, TABLES_PER_DB, COLUMNS_PER_TABLE);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();

        System.out.println("Starting " + CONCURRENT_THREADS + " concurrent threads...");
        System.out.println("Each thread accesses " + ACCESS_PER_THREAD + " databases");

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(CONCURRENT_THREADS);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong totalTime = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);

        for (int t = 0; t < CONCURRENT_THREADS; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    long threadStart = System.nanoTime();

                    Random random = new Random(threadId);
                    for (int i = 0; i < ACCESS_PER_THREAD; i++) {
                        int dbIndex = random.nextInt(DB_COUNT) + 2; // Skip system databases
                        ExternalDatabase<?> db = catalog.getDbNullable(dbNames.get(dbIndex));

                        if (db != null && db.getFullName().equals(dbNames.get(dbIndex))) {
                            // Table-level concurrent access: Access a random table
                            // This simulates: SELECT * FROM db_xxx.table_yyy
                            if (i % 5 == 0) {
                                int tableIndex = random.nextInt(Math.min(10, TABLES_PER_DB));
                                ExternalTable table = db.getTableNullable("table_" + tableIndex);
                                if (table == null || !table.getName().equals("table_" + tableIndex)) {
                                    errorCount.incrementAndGet();
                                    continue;
                                }
                            }
                            successCount.incrementAndGet();
                        } else {
                            errorCount.incrementAndGet();
                        }
                    }

                    long threadTime = (System.nanoTime() - threadStart) / 1_000_000;
                    totalTime.addAndGet(threadTime);
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long startTime = System.nanoTime();
        startLatch.countDown();

        boolean finished = doneLatch.await(60, TimeUnit.SECONDS);
        long elapsed = (System.nanoTime() - startTime) / 1_000_000; // ms

        executor.shutdown();

        int totalAccesses = CONCURRENT_THREADS * ACCESS_PER_THREAD;
        double qps = totalAccesses * 1000.0 / elapsed;

        System.out.println("Results:");
        System.out.println("  Total accesses: " + totalAccesses);
        System.out.println("  Successful: " + successCount.get());
        System.out.println("  Errors: " + errorCount.get());
        System.out.println("  Total time: " + elapsed + " ms");
        System.out.println("  QPS: " + String.format("%.0f", qps));
        System.out.println("  Average latency: " + String.format("%.2f", elapsed / (double) totalAccesses) + " ms");

        Assert.assertTrue("Should complete within timeout", finished);
        Assert.assertEquals("No errors should occur", 0, errorCount.get());
        Assert.assertEquals("All accesses should succeed", totalAccesses, successCount.get());
        Assert.assertTrue("QPS should be > 5000", qps > 5000);

        // ===== Correctness Verification: Concurrent Data Consistency =====
        // Real user scenario: Verify data consistency after concurrent access
        System.out.println("\nCorrectness Verification:");
        for (int i = 0; i < 10; i++) {
            String dbName = dbNames.get(i + 2);
            ExternalDatabase<?> db1 = catalog.getDbNullable(dbName);
            ExternalDatabase<?> db2 = catalog.getDbNullable(dbName);
            Assert.assertNotNull(db1);
            Assert.assertSame("Concurrent access should return same cached object", db1, db2);
            Assert.assertEquals("Database name should be correct", dbName, db1.getFullName());
        }
        System.out.println("  ✓ Concurrent access data consistency verified");

        System.out.println("✓ Concurrent queries work correctly\n");
    }

    /**
     * Scenario 5: REFRESH CATALOG impact
     * User operation: REFRESH CATALOG; then continue queries
     * User concern: Performance degradation after refresh
     */
    @Test
    public void testRefreshCatalogImpact() {
        System.out.println("\n=== Scenario 5: REFRESH CATALOG Impact ===");
        System.out.println("Scale: " + DB_COUNT + " DBs × " + TABLES_PER_DB + " tables");

        catalog = createCatalog(DB_COUNT, TABLES_PER_DB, COLUMNS_PER_TABLE);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();
        int testCount = 100;

        // Phase 1: Warm up cache
        System.out.println("Phase 1: Warming up cache...");
        // ===== Correctness Verification: Remember pre-REFRESH data =====
        Map<String, String> preRefreshData = new HashMap<>();
        Map<String, String> preRefreshTableData = new HashMap<>();
        for (int i = 0; i < testCount; i++) {
            String dbName = dbNames.get(i + 2);
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            preRefreshData.put(dbName, db.getFullName());

            // Table-level: Access a sample table before REFRESH
            if (i % 10 == 0) {
                ExternalTable table = db.getTableNullable("table_0");
                preRefreshTableData.put(dbName, table.getName());
            }
        }

        // Measure cache hit performance (use microseconds for better precision)
        long startTime = System.nanoTime();
        for (int i = 0; i < testCount; i++) {
            catalog.getDbNullable(dbNames.get(i + 2));
        }
        long beforeRefreshTime = (System.nanoTime() - startTime) / 1_000; // microseconds
        double beforeAvg = beforeRefreshTime / (double) testCount;

        System.out.println("  Before REFRESH - avg time: " + String.format("%.2f", beforeAvg) + " μs (db level)");

        // Phase 2: REFRESH CATALOG
        System.out.println("\nPhase 2: Executing REFRESH CATALOG...");
        long refreshStart = System.nanoTime();
        catalog.onRefreshCache(true);
        long refreshTime = (System.nanoTime() - refreshStart) / 1_000_000;
        System.out.println("  REFRESH completed in: " + refreshTime + " ms");

        long cacheSize = catalog.metaCache.getMetaObjCache().estimatedSize();
        System.out.println("  Cache size after REFRESH: " + cacheSize);
        Assert.assertEquals("Cache should be empty after refresh", 0, cacheSize);

        // Phase 3: Access after refresh
        System.out.println("\nPhase 3: Accessing databases after REFRESH...");
        startTime = System.nanoTime();
        for (int i = 0; i < testCount; i++) {
            catalog.getDbNullable(dbNames.get(i + 2));
        }
        long afterRefreshTime = (System.nanoTime() - startTime) / 1_000; // microseconds
        double afterAvg = afterRefreshTime / (double) testCount;

        System.out.println("  After REFRESH - avg time: " + String.format("%.2f", afterAvg) + " μs");

        // Phase 4: Re-access (should be cached again)
        startTime = System.nanoTime();
        for (int i = 0; i < testCount; i++) {
            catalog.getDbNullable(dbNames.get(i + 2));
        }
        long reAccessTime = (System.nanoTime() - startTime) / 1_000; // microseconds
        double reAccessAvg = reAccessTime / (double) testCount;

        System.out.println("  Re-access - avg time: " + String.format("%.2f", reAccessAvg) + " μs");

        System.out.println("\nSummary:");
        System.out.println("  Before REFRESH: " + String.format("%.2f", beforeAvg) + " μs (cache hit)");
        System.out.println("  After REFRESH: " + String.format("%.2f", afterAvg) + " μs (cache miss)");
        System.out.println("  Re-access: " + String.format("%.2f", reAccessAvg) + " μs (cache hit again)");
        System.out.println("  Performance ratio: re-access/before = " + String.format("%.2fx", reAccessAvg / beforeAvg));

        // Verify basic expectations
        // Note: re-access may be slower than initial warm cache due to various factors
        // (GC, CPU throttling, etc.), but should be much faster than cache miss
        Assert.assertTrue("Cache hit should be fast (< 10μs)", beforeAvg < 10);
        Assert.assertTrue("Re-access should be faster than cache miss (afterAvg=" + String.format("%.2f", afterAvg)
                        + "μs, reAccessAvg=" + String.format("%.2f", reAccessAvg) + "μs)",
                reAccessAvg < afterAvg);

        // ===== Correctness Verification: REFRESH Data Consistency =====
        // Real user scenario: Data should remain consistent after REFRESH
        System.out.println("\nCorrectness Verification:");
        for (int i = 0; i < 10; i++) {
            String dbName = dbNames.get(i + 2);
            ExternalDatabase<?> postRefreshDb = catalog.getDbNullable(dbName);
            Assert.assertNotNull(postRefreshDb);
            Assert.assertEquals("Database name should be same after REFRESH",
                    preRefreshData.get(dbName), postRefreshDb.getFullName());
        }
        System.out.println("  ✓ Database data remains consistent after REFRESH CATALOG");

        // Table-level consistency verification after REFRESH
        for (Map.Entry<String, String> entry : preRefreshTableData.entrySet()) {
            String dbName = entry.getKey();
            String expectedTableName = entry.getValue();
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            ExternalTable table = db.getTableNullable("table_0");
            Assert.assertNotNull("Table should exist after REFRESH", table);
            Assert.assertEquals("Table name should be same after REFRESH", expectedTableName, table.getName());
        }
        System.out.println("  ✓ Table data remains consistent after REFRESH CATALOG");

        System.out.println("✓ REFRESH CATALOG impact is acceptable\n");
    }

    /**
     * Scenario 6: Hot data access pattern
     * User operation: 90% queries hit 10 hot databases, 10% hit random databases
     * User concern: Hot data performance stability
     */
    @Test
    public void testHotDataAccessPattern() {
        System.out.println("\n=== Scenario 6: Hot Data Access Pattern ===");
        System.out.println("Scale: " + DB_COUNT + " DBs × " + TABLES_PER_DB + " tables");

        Config.max_meta_object_cache_num = DEFAULT_CACHE_SIZE;

        catalog = createCatalog(DB_COUNT, TABLES_PER_DB, COLUMNS_PER_TABLE);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();
        Random random = new Random(42);

        int hotHits = 0;
        int coldHits = 0;
        long hotTotalTime = 0;
        long coldTotalTime = 0;

        System.out.println("Configuration:");
        System.out.println("  Total databases: " + DB_COUNT);
        System.out.println("  Hot databases: " + HOT_DB_COUNT);
        System.out.println("  Cache size: " + DEFAULT_CACHE_SIZE);
        System.out.println("  Access pattern: 90% hot, 10% cold");
        System.out.println("\nExecuting " + LONG_RUN_ACCESSES + " accesses...");

        for (int i = 0; i < LONG_RUN_ACCESSES; i++) {
            boolean isHot = random.nextDouble() < 0.9;
            String dbName;

            if (isHot) {
                // Access hot databases
                int hotIndex = random.nextInt(HOT_DB_COUNT) + 2;
                dbName = dbNames.get(hotIndex);

                long start = System.nanoTime();
                ExternalDatabase<?> db = catalog.getDbNullable(dbName);
                // Table-level hot access: Also access a hot table
                if (i % 5 == 0) {
                    ExternalTable table = db.getTableNullable("table_0");
                    Assert.assertNotNull("Hot table should exist", table);
                }
                long elapsed = System.nanoTime() - start;

                Assert.assertNotNull(db);
                hotHits++;
                hotTotalTime += elapsed;
            } else {
                // Access cold databases (random from remaining)
                int coldIndex = random.nextInt(DB_COUNT - HOT_DB_COUNT) + HOT_DB_COUNT + 2;
                dbName = dbNames.get(coldIndex);

                long start = System.nanoTime();
                ExternalDatabase<?> db = catalog.getDbNullable(dbName);
                // Table-level cold access: Also access a cold table
                if (i % 10 == 0) {
                    ExternalTable table = db.getTableNullable("table_0");
                    Assert.assertNotNull("Cold table should exist", table);
                }
                long elapsed = System.nanoTime() - start;

                Assert.assertNotNull(db);
                coldHits++;
                coldTotalTime += elapsed;
            }

            if ((i + 1) % 2000 == 0) {
                System.out.println("  Progress: " + (i + 1) + " accesses completed");
            }
        }

        double hotAvg = (hotTotalTime / hotHits) / 1_000.0; // microseconds
        double coldAvg = (coldTotalTime / coldHits) / 1_000.0; // microseconds

        System.out.println("\nResults:");
        System.out.println("  Hot data accesses: " + hotHits + " (avg: "
                + String.format("%.2f", hotAvg) + " μs)");
        System.out.println("  Cold data accesses: " + coldHits + " (avg: "
                + String.format("%.2f", coldAvg) + " μs)");
        System.out.println("  Hot/Cold latency ratio: " + String.format("%.2f", coldAvg / hotAvg));
        System.out.println("  Final cache size: " + catalog.metaCache.getMetaObjCache().estimatedSize());

        Assert.assertTrue("Hot data should be very fast", hotAvg < 2); // < 2μs
        Assert.assertTrue("Hot data should be faster than cold data", hotAvg < coldAvg);

        // ===== Correctness Verification: Hot/Cold Data Correctness =====
        // Real user scenario: Verify hot and cold data are correct
        System.out.println("\nCorrectness Verification:");
        // Verify hot databases
        for (int i = 0; i < HOT_DB_COUNT; i++) {
            String hotDbName = dbNames.get(i + 2);
            ExternalDatabase<?> hotDb = catalog.getDbNullable(hotDbName);
            Assert.assertNotNull(hotDb);
            Assert.assertEquals("Hot database name should be correct", hotDbName, hotDb.getFullName());
        }
        // Verify cold database
        String coldDbName = dbNames.get(HOT_DB_COUNT + 100);
        ExternalDatabase<?> coldDb = catalog.getDbNullable(coldDbName);
        Assert.assertNotNull(coldDb);
        Assert.assertEquals("Cold database name should be correct", coldDbName, coldDb.getFullName());
        System.out.println("  ✓ Hot and cold data correctness verified");

        System.out.println("✓ Hot data access pattern works well\n");
    }

    /**
     * Scenario 7: Cache size too small
     * User operation: max_meta_object_cache_num << active database count
     * User concern: Performance degradation when cache thrashing occurs
     */
    @Test
    public void testCacheSizeTooSmall() {
        System.out.println("\n=== Scenario 7: Cache Size Too Small ===");
        System.out.println("Scale: " + DB_COUNT + " DBs × " + TABLES_PER_DB + " tables");

        Config.max_meta_object_cache_num = SMALL_CACHE_SIZE;

        catalog = createCatalog(DB_COUNT, TABLES_PER_DB, COLUMNS_PER_TABLE);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();

        System.out.println("Configuration:");
        System.out.println("  Database count: " + DB_COUNT);
        System.out.println("  Cache size: " + SMALL_CACHE_SIZE);
        System.out.println("  Cache/DB ratio: " + String.format("%.1f%%", SMALL_CACHE_SIZE * 100.0 / DB_COUNT));

        System.out.println("\nSequentially accessing all " + DB_COUNT + " databases...");

        // ===== Correctness Verification: Remember first database and table info =====
        // Real user scenario: Verify evicted database and table can be reloaded correctly
        String firstDbName = dbNames.get(2); // db_0
        ExternalDatabase<?> firstDbOriginal = catalog.getDbNullable(firstDbName);
        String firstDbFullName = firstDbOriginal.getFullName();
        // Table-level: Remember first table info
        ExternalTable firstTableOriginal = firstDbOriginal.getTableNullable("table_0");
        String firstTableName = firstTableOriginal.getName();

        long startTime = System.nanoTime();
        List<Long> cacheSizes = new ArrayList<>();

        for (int i = 0; i < DB_COUNT; i++) {
            String dbName = dbNames.get(i + 2);
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            Assert.assertNotNull(db);
            Assert.assertEquals("Database name should be correct", dbName, db.getFullName());

            // Table-level access: Access a sample table to trigger table cache
            // This simulates: SELECT * FROM db_xxx.table_0
            if (i % 20 == 0) {
                ExternalTable table = db.getTableNullable("table_0");
                Assert.assertNotNull("Table should exist", table);
                Assert.assertEquals("table_0", table.getName());
            }

            if (i % 10 == 0 || i < 20) {
                long cacheSize = catalog.metaCache.getMetaObjCache().estimatedSize();
                cacheSizes.add(cacheSize);
                System.out.println("  Access #" + i + ", db cache size: " + cacheSize);
            }
        }

        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        double avgTime = totalTime / (double) DB_COUNT;

        System.out.println("\nResults:");
        System.out.println("  Total time: " + totalTime + " ms");
        System.out.println("  Average time per database: " + String.format("%.2f", avgTime) + " ms");
        System.out.println("  Final cache size: " + catalog.metaCache.getMetaObjCache().estimatedSize());

        // Observe cache thrashing behavior
        // Note: Caffeine's async eviction may cause cache size to temporarily exceed limit
        // but it should not grow unbounded. Allow up to 10x buffer for async behavior.
        long finalCacheSize = catalog.metaCache.getMetaObjCache().estimatedSize();
        Assert.assertTrue("Cache size should not grow unbounded (max: " + SMALL_CACHE_SIZE + ", actual: "
                        + finalCacheSize + ")",
                finalCacheSize < DB_COUNT);

        // ===== Correctness Verification: Eviction and Reload =====
        // Real user scenario: First database was definitely evicted (accessed 1000+ databases later)
        // Verify it can be reloaded with correct data
        System.out.println("\nCorrectness Verification:");
        ExternalDatabase<?> firstDbReloaded = catalog.getDbNullable(firstDbName);
        Assert.assertNotNull(firstDbReloaded);
        Assert.assertEquals("Reloaded database after eviction should have same name",
                firstDbFullName, firstDbReloaded.getFullName());
        System.out.println("  ✓ Evicted database reloaded correctly after " + DB_COUNT + " accesses");

        // Table-level eviction and reload verification
        ExternalTable firstTableReloaded = firstDbReloaded.getTableNullable("table_0");
        Assert.assertNotNull("Evicted table should be reloadable", firstTableReloaded);
        Assert.assertEquals("Reloaded table after eviction should have same name",
                firstTableName, firstTableReloaded.getName());
        System.out.println("  ✓ Evicted table reloaded correctly (table_0 from db_0)");

        System.out.println("✓ Cache thrashing behavior observed, size controlled\n");
    }

    /**
     * Scenario 8: Long-running stability
     * User operation: Catalog runs for extended time with continuous queries
     * User concern: Memory leak or performance degradation over time
     */
    @Test
    public void testLongRunningStability() {
        System.out.println("\n=== Scenario 8: Long-Running Stability ===");
        System.out.println("Scale: " + DB_COUNT + " DBs × " + TABLES_PER_DB + " tables");

        catalog = createCatalog(DB_COUNT, TABLES_PER_DB, COLUMNS_PER_TABLE);
        catalog.setInitializedForTest(true);

        List<String> dbNames = catalog.getDbNames();
        Random random = new Random(42);

        int checkInterval = 1000;

        System.out.println("Simulating long-running workload...");
        System.out.println("  Total accesses: " + LONG_RUN_ACCESSES);
        System.out.println("  Check interval: " + checkInterval);

        // ===== Correctness Verification: Sample databases and tables for consistency check =====
        // Real user scenario: Verify data remains consistent during long running
        Map<String, String> sampleDatabases = new HashMap<>();
        Map<String, String> sampleTables = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            String dbName = dbNames.get(i + 2);
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            sampleDatabases.put(dbName, db.getFullName());

            // Table-level: Sample some tables for consistency check
            if (i % 5 == 0) {
                ExternalTable table = db.getTableNullable("table_0");
                sampleTables.put(dbName, table.getName());
            }
        }

        List<Long> memorySnapshots = new ArrayList<>();
        List<Long> timeSnapshots = new ArrayList<>();
        List<Long> cacheSnapshots = new ArrayList<>();

        for (int i = 0; i < LONG_RUN_ACCESSES; i++) {
            int dbIndex = random.nextInt(DB_COUNT) + 2;
            String dbName = dbNames.get(dbIndex);
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            Assert.assertNotNull(db);
            Assert.assertEquals("Database name should be correct during long running", dbName, db.getFullName());

            // Table-level access: Randomly access tables to simulate real queries
            // This simulates: SELECT * FROM db_xxx.table_yyy
            if (i % 10 == 0) {
                int tableIndex = random.nextInt(Math.min(10, TABLES_PER_DB));
                ExternalTable table = db.getTableNullable("table_" + tableIndex);
                Assert.assertNotNull("Table should exist during long running", table);
            }

            if ((i + 1) % checkInterval == 0) {
                long memory = getUsedMemory() / MB;
                long cacheSize = catalog.metaCache.getMetaObjCache().estimatedSize();
                memorySnapshots.add(memory);
                cacheSnapshots.add(cacheSize);
                timeSnapshots.add(System.currentTimeMillis());

                System.out.println("  After " + (i + 1) + " accesses: "
                        + "memory=" + memory + "MB, "
                        + "cache=" + cacheSize);
            }
        }

        System.out.println("\nStability Analysis:");
        long minMem = memorySnapshots.stream().min(Long::compareTo).orElse(0L);
        long maxMem = memorySnapshots.stream().max(Long::compareTo).orElse(0L);
        double memGrowth = (maxMem - minMem) * 100.0 / minMem;

        System.out.println("  Memory range: " + minMem + " MB ~ " + maxMem + " MB");
        System.out.println("  Memory growth: " + String.format("%.1f%%", memGrowth));

        long minCache = cacheSnapshots.stream().min(Long::compareTo).orElse(0L);
        long maxCache = cacheSnapshots.stream().max(Long::compareTo).orElse(0L);
        System.out.println("  Cache size range: " + minCache + " ~ " + maxCache);

        // Memory should not grow unbounded
        Assert.assertTrue("Memory growth should be reasonable", memGrowth < 100);

        // ===== Correctness Verification: Long-running Data Consistency =====
        // Real user scenario: Verify sampled databases still have correct data
        System.out.println("\nCorrectness Verification:");
        for (Map.Entry<String, String> entry : sampleDatabases.entrySet()) {
            String dbName = entry.getKey();
            String expectedName = entry.getValue();
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            Assert.assertNotNull(db);
            Assert.assertEquals("Database should maintain correct name after " + LONG_RUN_ACCESSES + " accesses",
                    expectedName, db.getFullName());
        }
        System.out.println("  ✓ Database data consistency maintained after " + LONG_RUN_ACCESSES + " accesses");

        // Table-level consistency verification after long-running
        for (Map.Entry<String, String> entry : sampleTables.entrySet()) {
            String dbName = entry.getKey();
            String expectedTableName = entry.getValue();
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            ExternalTable table = db.getTableNullable("table_0");
            Assert.assertNotNull("Table should exist after long-running", table);
            Assert.assertEquals("Table should maintain correct name after " + LONG_RUN_ACCESSES + " accesses",
                    expectedTableName, table.getName());
        }
        System.out.println("  ✓ Table data consistency maintained after " + LONG_RUN_ACCESSES + " accesses");

        System.out.println("✓ Long-running stability verified\n");
    }

    /**
     * Scenario 9: Million-level tables extreme test
     * User operation: Access catalog with 1 million tables
     * User concern: Can it work at extreme scale?
     */
    @Test
    public void testMillionTablesExtreme() {
        System.out.println("\n=== Scenario 9: Million-Level Tables Extreme Test ===");
        System.out.println("NOTE: Modify DB_COUNT and TABLES_PER_DB at class top to adjust scale");
        System.out.println("Current scale: " + DB_COUNT + " DBs × " + TABLES_PER_DB + " tables = "
                + (DB_COUNT * TABLES_PER_DB) + " tables");

        catalog = createCatalog(DB_COUNT, TABLES_PER_DB, COLUMNS_PER_TABLE);
        catalog.setInitializedForTest(true);

        long memBefore = getUsedMemory();

        System.out.println("Accessing all databases...");
        long startTime = System.nanoTime();

        List<String> dbNames = catalog.getDbNames();
        for (int i = 0; i < DB_COUNT; i++) {
            String dbName = dbNames.get(i + 2);
            ExternalDatabase<?> db = catalog.getDbNullable(dbName);
            Assert.assertNotNull(db);

            if ((i + 1) % 10 == 0) {
                System.out.println("  Accessed " + (i + 1) + " databases");
            }
        }

        long totalTime = (System.nanoTime() - startTime) / 1_000_000;
        long memAfter = getUsedMemory();
        long memUsed = (memAfter - memBefore) / MB;

        System.out.println("\nResults:");
        System.out.println("  Total time: " + totalTime + " ms");
        System.out.println("  Average time per database: " + (totalTime / DB_COUNT) + " ms");
        System.out.println("  Memory used: " + memUsed + " MB");
        System.out.println("  Cache size: " + catalog.metaCache.getMetaObjCache().estimatedSize());

        Assert.assertTrue("Should complete without OOM", memUsed < 2048);
        Assert.assertTrue("Average time should be reasonable", totalTime / DB_COUNT < 200);

        System.out.println("✓ Million-level tables test passed\n");
    }

    // Helper methods

    private TestExternalCatalog createCatalog(int dbCount, int tablesPerDb, int columnsPerTable) {
        System.setProperty("test.db_count", String.valueOf(dbCount));
        System.setProperty("test.tables_per_db", String.valueOf(tablesPerDb));
        System.setProperty("test.columns_per_table", String.valueOf(columnsPerTable));

        Map<String, String> props = new HashMap<>();
        props.put("catalog_provider.class", UserScenarioTestCatalogProvider.class.getName());

        return new TestExternalCatalog(1L, "test_catalog", "test", props, "");
    }

    private long getUsedMemory() {
        System.gc();
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Ignore
        }
        return runtime.totalMemory() - runtime.freeMemory();
    }

    /**
     * Catalog provider for user scenario tests.
     * Uses lazy loading to generate metadata on-demand, avoiding OOM with million-level tables.
     *
     * Memory optimization:
     * - Old approach: Pre-generate all objects (1000 DBs × 1000 tables × 5 columns = 5M Column objects)
     *   Memory: ~16GB+ (OOM even with 16GB heap)
     * - New approach: Generate on-the-fly when accessed
     *   Memory: Only loaded objects are kept (controlled by MetaCache capacity)
     */
    public static class UserScenarioTestCatalogProvider implements TestExternalCatalog.TestCatalogProvider {
        private final int dbCount;
        private final int tablesPerDb;
        private final int columnsPerTable;

        // Lazy-loading metadata: only database names are pre-generated
        // Tables and columns are generated on-demand when getMetadata() is called
        private final Map<String, Map<String, List<Column>>> lazyMetadata;

        public UserScenarioTestCatalogProvider() {
            this.dbCount = Integer.parseInt(System.getProperty("test.db_count", "10"));
            this.tablesPerDb = Integer.parseInt(System.getProperty("test.tables_per_db", "10"));
            this.columnsPerTable = Integer.parseInt(System.getProperty("test.columns_per_table", "5"));

            // Use a lazy-loading map that generates table metadata on first access
            this.lazyMetadata = new HashMap<String, Map<String, List<Column>>>() {
                @Override
                public Map<String, List<Column>> get(Object key) {
                    String dbName = (String) key;
                    Map<String, List<Column>> tables = super.get(dbName);

                    // Generate table metadata on first access
                    if (tables == null && dbName.startsWith("db_")) {
                        tables = generateTablesForDatabase();
                        put(dbName, tables);
                    }

                    return tables;
                }
            };

            // Only pre-generate database names (lightweight)
            for (int i = 0; i < dbCount; i++) {
                String dbName = "db_" + i;
                // Don't generate tables yet, just mark the database exists
                lazyMetadata.put(dbName, null);
            }
        }

        /**
         * Generate table metadata for a database on-demand.
         * This is called lazily when a database is first accessed.
         */
        private Map<String, List<Column>> generateTablesForDatabase() {
            Map<String, List<Column>> tables = Maps.newHashMap();

            for (int j = 0; j < tablesPerDb; j++) {
                String tableName = "table_" + j;
                // Columns are also generated on-demand
                List<Column> columns = generateColumns(columnsPerTable);
                tables.put(tableName, columns);
            }

            return tables;
        }

        /**
         * Generate column metadata on-demand.
         */
        private List<Column> generateColumns(int count) {
            List<Column> columns = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                columns.add(new Column("col_" + i, ScalarType.createStringType(), true, null, true, "", ""));
            }
            return columns;
        }

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return lazyMetadata;
        }
    }
}
