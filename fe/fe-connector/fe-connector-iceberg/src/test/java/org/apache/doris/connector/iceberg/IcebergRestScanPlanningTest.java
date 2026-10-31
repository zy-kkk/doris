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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.iceberg.IcebergStatementScopeTest.ScopeSession;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.kerberos.HadoopAuthenticator;

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SupportsDistributedScanPlanning;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ConfigResponseParser;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Exercises actual SDK RESTTable capabilities across Doris read ownership boundaries. */
public class IcebergRestScanPlanningTest {
    private static final TableIdentifier ID = TableIdentifier.of("db", "t");

    @Test
    public void directReadLendsAViewAndDoesNotPlan() throws Exception {
        for (boolean serverOverride : new boolean[] {false, true}) {
            for (boolean populated : new boolean[] {false, true}) {
                try (RestFixture fixture = new RestFixture(serverOverride, populated, true)) {
                    Table table = fixture.load();
                    Assertions.assertInstanceOf(BaseTable.class, table);
                    Assertions.assertFalse(((SupportsDistributedScanPlanning) table).allowDistributedPlanning());
                    for (ConnectorStatementScope scope : List.of(ConnectorStatementScope.NONE,
                            new TestStatementScope())) {
                        ConnectorSession session = new ScopeSession(1L, "q", scope);
                        Table first = IcebergStatementScope.sharedTable(session, "db", "t", () -> table);
                        assertReadView(table, first);
                        if (scope == ConnectorStatementScope.NONE) {
                            // NONE runs the loader on every call, so each call gets its own view.
                            Table again = IcebergStatementScope.sharedTable(session, "db", "t", () -> table);
                            assertReadView(table, again);
                            Assertions.assertNotSame(first, again);
                            Table borrowed = IcebergStatementScope.sharedBorrowedTable(session, "db", "t", () -> {
                                throw new AssertionError("NONE must use direct loader");
                            }, () -> table);
                            assertReadView(table, borrowed);
                            Assertions.assertNotSame(first, borrowed);
                        } else {
                            Table second = IcebergStatementScope.sharedTable(session, "db", "t", () -> {
                                throw new AssertionError("one statement must reuse the read view");
                            });
                            Assertions.assertSame(first, second, "self-join sides share one statement view");
                        }
                        scope.closeAll();
                    }
                    Assertions.assertEquals(0, fixture.io(table).reads.get());
                    Assertions.assertEquals(0, fixture.planRequests.get());
                }
            }
        }
    }

    @Test
    public void cacheHoldsSdkTableAndLendsReadView() throws Exception {
        for (boolean serverOverride : new boolean[] {false, true}) {
            for (boolean populated : new boolean[] {false, true}) {
                try (RestFixture fixture = new RestFixture(serverOverride, populated, true)) {
                    for (CacheSpec spec : List.of(CacheSpec.ofConnectorTtl(0, 10),
                            CacheSpec.ofConnectorTtl(100, 10),
                            CacheSpec.ofWeight(true, 100, 1000, 10L * 1024 * 1024))) {
                        AtomicInteger tableCleanups = new AtomicInteger();
                        AtomicInteger catalogCleanups = new AtomicInteger();
                        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
                        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
                            IcebergTableCache cache = new IcebergTableCache(owner, spec,
                                    ignored -> tableCleanups::incrementAndGet, tracker);
                            Table table = fixture.load();
                            try (IcebergTableCache.TableLease lease = cache.borrow(ID, () -> table)) {
                                Assertions.assertSame(table, lease.table(), "the cache keeps the SDK table");
                                Table view = lease.snapshotReadTable();
                                assertReadView(table, view);
                                Assertions.assertNotSame(view, lease.snapshotReadTable(),
                                        "each lend builds its own pinned view over the cached SDK table");
                            }
                            if (cache.isEnabled()) {
                                Assertions.assertEquals(1, cache.size());
                                Assertions.assertEquals(0, tableCleanups.get(),
                                        "an enabled cache keeps the owner until it drops the entry");
                            }
                            Assertions.assertEquals(0, fixture.io(table).reads.get());
                            cache.close();
                            tracker.close(catalogCleanups::incrementAndGet);
                            Assertions.assertEquals(1, tableCleanups.get());
                            Assertions.assertEquals(1, catalogCleanups.get());
                            Assertions.assertEquals(0, tracker.retainedCleanupCount());
                        }
                    }
                }
            }
        }
    }

    @Test
    public void trackedReadsReleaseRejectedTablesButWritableTablesRemainAvailable() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            for (ConnectorStatementScope scope : List.of(ConnectorStatementScope.NONE, new TestStatementScope())) {
                ConnectorSession session = new ScopeSession(1L, "q", scope);
                IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
                AtomicInteger tableCleanups = new AtomicInteger();
                AtomicInteger catalogCleanups = new AtomicInteger();
                Table table = fixture.load();
                if (scope == ConnectorStatementScope.NONE) {
                    // The action sees the view; closing the tracked owner is what releases the load.
                    Table view = IcebergStatementScope.withTrackedTable(session, "db", "t", tracker,
                            () -> table, ignored -> tableCleanups::incrementAndGet, value -> value);
                    assertReadView(table, view);
                } else {
                    reject(() -> IcebergStatementScope.withTrackedTable(session, "db", "t", tracker,
                            () -> table, ignored -> tableCleanups::incrementAndGet, value -> value));
                }
                Assertions.assertEquals(1, tableCleanups.get());
                Assertions.assertSame(table,
                        IcebergStatementScope.sharedWritableTable(session, "db", "t", () -> table));
                scope.closeAll();
                tracker.close(catalogCleanups::incrementAndGet);
                Assertions.assertEquals(1, tableCleanups.get(), "failed scope load must not retain a close callback");
                Assertions.assertEquals(1, catalogCleanups.get());
                Assertions.assertEquals(0, fixture.io(table).reads.get());
            }
        }
    }

    @Test
    public void unscopedDirectRejectionReleasesTableOwnedResources() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            Table table = fixture.load();
            AtomicInteger cleanups = new AtomicInteger();
            reject(() -> IcebergStatementScope.sharedTrackedTable(null, "db", "t",
                    new IcebergCatalogResourceTracker(), () -> table, ignored -> cleanups::incrementAndGet));
            Assertions.assertEquals(1, cleanups.get());
            Assertions.assertEquals(0, fixture.io(table).reads.get());
        }
    }

    @Test
    public void clientPlanningStillCreatesPinnedReadViews() throws Exception {
        try (RestFixture fixture = new RestFixture(false, true, false);
                CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            Table table = fixture.load();
            Assertions.assertTrue(((SupportsDistributedScanPlanning) table).allowDistributedPlanning());
            Table direct = IcebergStatementScope.sharedTable(null, "db", "t", () -> table);
            Assertions.assertNotSame(table, direct);
            Assertions.assertEquals(table.currentSnapshot().snapshotId(), direct.currentSnapshot().snapshotId());
            IcebergTableCache cache = new IcebergTableCache(owner, 100, 10, ignored -> () -> { }, null);
            try (IcebergTableCache.TableLease lease = cache.borrow(ID, () -> table)) {
                Table cached = lease.snapshotReadTable();
                Assertions.assertNotSame(table, cached);
                Assertions.assertEquals(direct.currentSnapshot().snapshotId(), cached.currentSnapshot().snapshotId());
            }
            cache.close();
            Assertions.assertEquals(0, fixture.io(table).reads.get());
        }
    }

    @Test
    public void scanAndMetadataTableRejectThroughBothAcquisitionPaths() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            for (boolean cached : new boolean[] {false, true}) {
                for (ConnectorStatementScope scope : List.of(ConnectorStatementScope.NONE, new TestStatementScope())) {
                    ConnectorSession session = new ScopeSession(1L, "q", scope);
                    Table table = fixture.load();
                    RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
                    ops.table = table;
                    IcebergTableCache cache = new IcebergTableCache(100, 10);
                    try {
                        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                                IcebergCatalogProperties.of(Map.of()), ignored -> ops, null, null,
                                cached ? cache : null);
                        reject(() -> provider.planScan(session, ConnectorScanRequest.builder(
                                new IcebergTableHandle("db", "t"), List.of()).build()));
                        reject(() -> provider.planScan(session, ConnectorScanRequest.builder(
                                IcebergTableHandle.forSystemTable("db", "t", "files", -1L, null, -1L),
                                List.of()).build()));
                        Assertions.assertEquals(0, fixture.io(table).reads.get());
                    } finally {
                        scope.closeAll();
                        cache.close();
                    }
                }
            }
            Assertions.assertEquals(0, fixture.planRequests.get());
        }
    }

    @Test
    public void kerberosRewritePlanningAsksTheServerForTasks() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            Table table = fixture.load();
            RecordingIcebergCatalogOps ops = new RecordingIcebergCatalogOps();
            ops.table = table;
            IcebergProcedureOps procedureOps = new IcebergProcedureOps(Map.of(), ops, new TcclPinningConnectorContext(
                    new RecordingConnectorContext(), getClass().getClassLoader(), InlineAuthenticator::new));

            // The fixture answers every plan request with HTTP 500, so planning fails; the test is where it went.
            Assertions.assertThrows(DorisConnectorException.class, () -> procedureOps.planRewrite(null,
                    new IcebergTableHandle("db", "t"), "rewrite_data_files", Map.of("rewrite-all", "true"), null,
                    List.of()));
            // MUTATION: planning wrapTableForProcedure(table) -> the plain BaseTable plans from local manifests ->
            // the recording FileIO fails the read and no plan request reaches the server -> red.
            Assertions.assertEquals(1, fixture.planRequests.get());
            Assertions.assertEquals(0, fixture.io(table).reads.get());
        }
    }

    @Test
    public void selfJoinKeepsOnePinnedViewAfterTheSdkTableRefreshes() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            AtomicInteger cleanups = new AtomicInteger();
            IcebergTableCache cache = new IcebergTableCache(100, 10, ignored -> cleanups::incrementAndGet);
            TestStatementScope scope = new TestStatementScope();
            ConnectorSession session = new ScopeSession(1L, "join", scope);
            Table left = IcebergStatementScope.sharedBorrowedTable(session, "db", "t",
                    () -> cache.borrow(ID, fixture::load), () -> {
                        throw new AssertionError("statement scope must borrow");
                    });
            Table right = IcebergStatementScope.sharedBorrowedTable(session, "db", "t",
                    () -> {
                        throw new AssertionError("self-join must not borrow a second lease");
                    }, () -> {
                        throw new AssertionError("statement scope must not reload");
                    });
            Assertions.assertSame(left, right);
            assertReadView(left.newScan().table(), left);
            long pinnedSnapshot = left.currentSnapshot().snapshotId();
            Table sdk = left.newScan().table();
            fixture.appendDataFile();
            ((BaseTable) sdk).refresh();
            Assertions.assertNotEquals(pinnedSnapshot, sdk.currentSnapshot().snapshotId());
            Assertions.assertEquals(pinnedSnapshot, left.currentSnapshot().snapshotId());
            Assertions.assertEquals(pinnedSnapshot, right.newScan().snapshot().snapshotId());
            Assertions.assertSame(sdk, right.newScan().table());
            Assertions.assertEquals(0, fixture.io(sdk).reads.get());
            Assertions.assertEquals(0, fixture.planRequests.get());

            ConnectorSession otherSession = new ScopeSession(2L, "other", new TestStatementScope());
            Table other = IcebergStatementScope.sharedBorrowedTable(otherSession, "db", "t",
                    () -> cache.borrow(ID, () -> {
                        throw new AssertionError("the SDK table is already cached");
                    }), () -> {
                        throw new AssertionError("cache hit");
                    });
            Assertions.assertNotSame(left, other);
            // This cache is not weight-bounded, so a new statement pins the SDK table's current generation.
            Assertions.assertEquals(sdk.currentSnapshot().snapshotId(), other.currentSnapshot().snapshotId());
            Assertions.assertTrue(IcebergScanPlanning.requiresServerSideScanPlanning(other));
            scope.closeAll();
            ((TestStatementScope) otherSession.getStatementScope()).closeAll();
            Assertions.assertEquals(0, cleanups.get(), "open statement leases keep the cached table owner alive");
            cache.close();
            Assertions.assertEquals(1, cleanups.get());
        }
    }

    @Test
    public void weightedCacheLendsTheAdmittedGeneration() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true);
                CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            IcebergTableCache cache = new IcebergTableCache(owner,
                    CacheSpec.ofWeight(true, 100, 1000, 10L * 1024 * 1024),
                    ignored -> () -> { }, null);
            try (IcebergTableCache.TableLease lease = cache.borrow(ID, fixture::load)) {
                Table sdk = lease.table();
                long admitted = sdk.currentSnapshot().snapshotId();
                Table first = lease.snapshotReadTable();
                fixture.appendDataFile();
                ((BaseTable) sdk).refresh();
                Table second = lease.snapshotReadTable();
                Assertions.assertNotEquals(admitted, sdk.currentSnapshot().snapshotId());
                Assertions.assertEquals(admitted, first.currentSnapshot().snapshotId());
                Assertions.assertEquals(admitted, second.currentSnapshot().snapshotId());
                Assertions.assertNotSame(first.currentSnapshot(), sdk.currentSnapshot());
                Assertions.assertSame(sdk, first.newScan().table());
                Assertions.assertEquals(0, fixture.io(sdk).reads.get());
            }
            cache.close();
        }
    }

    @Test
    public void scanHandleUsesRefOrSnapshotThenProjects() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            long tagged = fixture.tagHead("hour");
            int taggedSchema = fixture.currentSchemaId();
            fixture.appendDataFile();
            fixture.renameIdColumn();
            Table sdk = fixture.load();
            Table view = IcebergServerPlannedReadView.pin(sdk);
            Assertions.assertNotEquals(tagged, sdk.currentSnapshot().snapshotId());

            IcebergTableHandle byRef = new IcebergTableHandle("db", "t")
                    .withSnapshot(tagged, "hour", taggedSchema);
            Schema historical = view.schemas().get(taggedSchema);
            TableScan refScan = IcebergServerPlannedReadView.configureScan(view, byRef, historical);
            Assertions.assertEquals("org.apache.iceberg.rest.RESTTableScan", refScan.getClass().getName());
            Assertions.assertSame(sdk, refScan.table());
            Assertions.assertEquals(tagged, refScan.snapshot().snapshotId());
            Assertions.assertTrue(refScan.schema().sameSchema(historical));
            Assertions.assertNotNull(refScan.schema().findField("id"));
            Assertions.assertNull(refScan.schema().findField("renamed_id"));

            IcebergTableHandle byId = new IcebergTableHandle("db", "t")
                    .withSnapshot(tagged, null, taggedSchema);
            TableScan idScan = IcebergServerPlannedReadView.configureScan(view, byId, historical);
            Assertions.assertEquals("org.apache.iceberg.rest.RESTTableScan", idScan.getClass().getName());
            Assertions.assertEquals(tagged, idScan.snapshot().snapshotId());
            Assertions.assertTrue(idScan.schema().sameSchema(historical));
            Assertions.assertEquals(0, fixture.planRequests.get());
            Assertions.assertEquals(0, fixture.io(sdk).reads.get());
        }
    }

    @Test
    public void emptyPinRefusesASnapshotCommittedAfterTheView() throws Exception {
        try (RestFixture fixture = new RestFixture(true, false, true)) {
            Table sdk = fixture.load();
            Table view = IcebergServerPlannedReadView.pin(sdk);
            Assertions.assertNull(view.currentSnapshot());
            Assertions.assertNull(view.newScan().snapshot());
            fixture.appendDataFile();
            ((BaseTable) sdk).refresh();
            DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class, view::newScan);
            Assertions.assertTrue(failure.getMessage().contains("pinned with no snapshot"), failure.getMessage());
            Assertions.assertEquals(0, fixture.planRequests.get());
        }
    }

    @Test
    public void kerberosWrapDoesNotRebuildServerTables() throws Exception {
        try (RestFixture fixture = new RestFixture(true, true, true)) {
            Table sdk = fixture.load();
            Table view = IcebergServerPlannedReadView.pin(sdk);
            IcebergScanPlanProvider provider = new IcebergScanPlanProvider(
                    IcebergCatalogProperties.of(Map.of()), ignored -> new RecordingIcebergCatalogOps(),
                    new TcclPinningConnectorContext(new RecordingConnectorContext(), getClass().getClassLoader(),
                            InlineAuthenticator::new), null);
            Assertions.assertSame(sdk, provider.wrapTableForScan(sdk));
            Assertions.assertSame(view, provider.wrapTableForScan(view));
            Assertions.assertTrue(IcebergScanPlanning.requiresServerSideScanPlanning(
                    provider.wrapTableForScan(view)));
            Assertions.assertEquals(0, fixture.io(sdk).reads.get());
        }
    }

    private static void assertReadView(Table sdk, Table view) {
        Assertions.assertNotSame(sdk, view);
        Assertions.assertFalse(view instanceof BaseTable);
        Assertions.assertTrue(IcebergScanPlanning.requiresServerSideScanPlanning(view));
        Assertions.assertFalse(((SupportsDistributedScanPlanning) view).allowDistributedPlanning());
        Assertions.assertSame(sdk.io(), view.io());
        TableScan scan = view.newScan();
        Assertions.assertEquals("org.apache.iceberg.rest.RESTTableScan", scan.getClass().getName());
        Assertions.assertSame(sdk, scan.table());
        if (sdk.currentSnapshot() == null) {
            Assertions.assertNull(view.currentSnapshot());
            Assertions.assertNull(scan.snapshot());
        } else {
            Assertions.assertEquals(sdk.currentSnapshot().snapshotId(), view.currentSnapshot().snapshotId());
            Assertions.assertEquals(sdk.currentSnapshot().snapshotId(), scan.snapshot().snapshotId());
        }
        Assertions.assertTrue(view.schema().sameSchema(sdk.schema()));
        Assertions.assertEquals(sdk.spec().specId(), view.spec().specId());
    }

    private static void reject(Executable action) {
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class, action);
        Assertions.assertEquals("Iceberg server-side scan planning is not supported for table db.t; "
                + "configure the REST catalog to use client-side scan planning", failure.getMessage());
    }

    /** A minimal real HTTP catalog; any attempt to read local manifests fails immediately. */
    private static final class RestFixture implements AutoCloseable {
        private final HttpServer server;
        private final RESTCatalog catalog = new RESTCatalog();
        private final InMemoryCatalog storage = new InMemoryCatalog();
        private final AtomicInteger planRequests = new AtomicInteger();
        private final boolean serverOverride;
        private final AtomicInteger files = new AtomicInteger();
        private volatile byte[] loadResponse;

        private RestFixture(boolean serverOverride, boolean populated, boolean serverPlanning) throws IOException {
            this.serverOverride = serverOverride;
            storage.initialize("storage", Map.of());
            storage.createNamespace(Namespace.of("db"));
            storage.createTable(ID, new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
            if (populated) {
                appendDataFile();
            } else {
                publish();
            }
            byte[] config = ConfigResponseParser.toJson(ConfigResponse.builder()
                    .withEndpoints(List.of(Endpoint.V1_LOAD_TABLE, Endpoint.V1_SUBMIT_TABLE_SCAN_PLAN)).build())
                    .getBytes(StandardCharsets.UTF_8);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                byte[] response;
                int status = 200;
                if (path.equals("/v1/config")) {
                    response = config;
                } else if (path.equals("/v1/namespaces/db/tables/t")) {
                    response = loadResponse;
                } else {
                    planRequests.incrementAndGet();
                    response = new byte[0];
                    status = 500;
                }
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, response.length);
                try (var out = exchange.getResponseBody()) {
                    out.write(response);
                }
            });
            server.start();
            catalog.initialize("rest", Map.of("uri", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "scan-planning-mode", serverPlanning && !serverOverride ? "server" : "client",
                    "rest.auth.type", "none", "io-impl", RecordingFileIO.class.getName()));
        }

        private Table load() {
            return catalog.loadTable(ID);
        }

        private RecordingFileIO io(Table table) {
            return (RecordingFileIO) table.io();
        }

        private long currentSnapshotId() {
            return storage.loadTable(ID).currentSnapshot().snapshotId();
        }

        private int currentSchemaId() {
            return storage.loadTable(ID).schema().schemaId();
        }

        private void appendDataFile() {
            Table table = storage.loadTable(ID);
            int file = files.incrementAndGet();
            table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                    .withPath("memory://data-" + file + ".parquet").withRecordCount(4).withFileSizeInBytes(100)
                    .build()).commit();
            publish();
        }

        private long tagHead(String name) {
            Table table = storage.loadTable(ID);
            long snapshotId = table.currentSnapshot().snapshotId();
            table.manageSnapshots().createTag(name, snapshotId).commit();
            publish();
            return snapshotId;
        }

        private void renameIdColumn() {
            Table table = storage.loadTable(ID);
            table.updateSchema().renameColumn("id", "renamed_id").commit();
            publish();
        }

        private void publish() {
            Table table = storage.loadTable(ID);
            LoadTableResponse.Builder response = LoadTableResponse.builder()
                    .withTableMetadata(((BaseTable) table).operations().current());
            if (serverOverride) {
                response.addConfig("scan-planning-mode", "server");
            }
            loadResponse = LoadTableResponseParser.toJson(response.build()).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void close() throws IOException {
            try {
                catalog.close();
            } finally {
                try {
                    storage.close();
                } finally {
                    server.stop(0);
                }
            }
        }
    }

    /** A Kerberos-catalog stand-in: runs the action inline, without a real UGI. */
    private static final class InlineAuthenticator implements HadoopAuthenticator {
        @Override
        public UserGroupInformation getUGI() {
            throw new UnsupportedOperationException("doAs is overridden; no real UGI in this test");
        }

        @Override
        public <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException {
            try {
                return action.run();
            } catch (IOException | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    public static class RecordingFileIO implements FileIO {
        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public InputFile newInputFile(String path) {
            reads.incrementAndGet();
            throw new AssertionError("Unexpected local metadata read: " + path);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }
    }

}
