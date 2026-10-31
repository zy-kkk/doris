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

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Labels;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.ReplaceSortOrder;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.SupportsDistributedScanPlanning;
import org.apache.iceberg.SupportsLabels;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateLocation;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Statement read of a server-planned REST table.
 *
 * <p>{@code RESTTable} is package-private, so this view keeps the SDK instance and forwards
 * {@link #newScan()} to it. Rebuilding a {@link BaseTable} would turn
 * {@link SupportsDistributedScanPlanning#allowDistributedPlanning()} back to true and plan from
 * local manifests. The view reports that capability as false, pins one metadata generation for
 * schema and snapshot reads, and applies the statement handle on the SDK scan.</p>
 *
 * <p>The view does not close the delegate {@link FileIO}. Cache leases and direct-load cleanup
 * still own that release.</p>
 */
final class IcebergServerPlannedReadView implements Table, SupportsDistributedScanPlanning, SupportsLabels {

    private final Table delegate;
    private final TableMetadata pinned;
    private final long pinnedSnapshotId;
    private final Labels labels;

    private IcebergServerPlannedReadView(Table delegate, TableMetadata metadata) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.pinned = detach(delegate, metadata);
        Snapshot current = this.pinned == null ? null : this.pinned.currentSnapshot();
        this.pinnedSnapshotId = current == null ? -1L : current.snapshotId();
        Labels loaded = delegate instanceof SupportsLabels ? ((SupportsLabels) delegate).labels() : null;
        this.labels = loaded == null ? Labels.EMPTY : loaded;
    }

    /** Pins {@code table}'s current metadata and forwards scans to that same instance. */
    static IcebergServerPlannedReadView pin(Table table) {
        if (table instanceof IcebergServerPlannedReadView) {
            return (IcebergServerPlannedReadView) table;
        }
        TableMetadata metadata = table instanceof BaseTable ? ((BaseTable) table).operations().current() : null;
        return new IcebergServerPlannedReadView(table, metadata);
    }

    /**
     * Pins {@code metadata} when the cache already detached it. {@code metadata} may still be the
     * live object; the constructor copies it unless doing so would fetch an unloaded snapshot list.
     */
    static IcebergServerPlannedReadView pin(Table table, TableMetadata metadata) {
        if (table instanceof IcebergServerPlannedReadView) {
            return (IcebergServerPlannedReadView) table;
        }
        return new IcebergServerPlannedReadView(table, metadata);
    }

    /**
     * One SDK scan configured for {@code handle}: {@code useRef} or {@code useSnapshot}, then
     * {@code project}. Callers must not apply that pin again; Iceberg rejects a second snapshot.
     */
    static TableScan configureScan(Table table, IcebergTableHandle handle, Schema selectedSchema) {
        return pin(table).configure(handle, selectedSchema);
    }

    @Override
    public boolean allowDistributedPlanning() {
        return false;
    }

    @Override
    public Labels labels() {
        return labels;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public void refresh() {
        // Slot binding and the later scan share this generation. Refresh stays on the SDK table.
    }

    @Override
    public TableScan newScan() {
        Schema selected = pinned == null ? null : pinned.schema();
        return configure(null, selected);
    }

    @Override
    public Schema schema() {
        return pinned != null ? pinned.schema() : delegate.schema();
    }

    @Override
    public Map<Integer, Schema> schemas() {
        return pinned != null ? pinned.schemasById() : delegate.schemas();
    }

    @Override
    public PartitionSpec spec() {
        return pinned != null ? pinned.spec() : delegate.spec();
    }

    @Override
    public Map<Integer, PartitionSpec> specs() {
        return pinned != null ? pinned.specsById() : delegate.specs();
    }

    @Override
    public SortOrder sortOrder() {
        return pinned != null ? pinned.sortOrder() : delegate.sortOrder();
    }

    @Override
    public Map<Integer, SortOrder> sortOrders() {
        return pinned != null ? pinned.sortOrdersById() : delegate.sortOrders();
    }

    @Override
    public Map<String, String> properties() {
        return pinned != null ? pinned.properties() : delegate.properties();
    }

    @Override
    public String location() {
        return pinned != null ? pinned.location() : delegate.location();
    }

    @Override
    public Snapshot currentSnapshot() {
        return pinned != null ? pinned.currentSnapshot() : delegate.currentSnapshot();
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        return pinned != null ? pinned.snapshot(snapshotId) : delegate.snapshot(snapshotId);
    }

    @Override
    public Iterable<Snapshot> snapshots() {
        return pinned != null ? pinned.snapshots() : delegate.snapshots();
    }

    @Override
    public List<HistoryEntry> history() {
        return pinned != null ? pinned.snapshotLog() : delegate.history();
    }

    @Override
    public UpdateSchema updateSchema() {
        return readOnly();
    }

    @Override
    public UpdatePartitionSpec updateSpec() {
        return readOnly();
    }

    @Override
    public UpdateProperties updateProperties() {
        return readOnly();
    }

    @Override
    public ReplaceSortOrder replaceSortOrder() {
        return readOnly();
    }

    @Override
    public UpdateLocation updateLocation() {
        return readOnly();
    }

    @Override
    public AppendFiles newAppend() {
        return readOnly();
    }

    @Override
    public RewriteFiles newRewrite() {
        return readOnly();
    }

    @Override
    public RewriteManifests rewriteManifests() {
        return readOnly();
    }

    @Override
    public OverwriteFiles newOverwrite() {
        return readOnly();
    }

    @Override
    public RowDelta newRowDelta() {
        return readOnly();
    }

    @Override
    public ReplacePartitions newReplacePartitions() {
        return readOnly();
    }

    @Override
    public DeleteFiles newDelete() {
        return readOnly();
    }

    @Override
    public ExpireSnapshots expireSnapshots() {
        return readOnly();
    }

    @Override
    public ManageSnapshots manageSnapshots() {
        return readOnly();
    }

    @Override
    public Transaction newTransaction() {
        return readOnly();
    }

    @Override
    public FileIO io() {
        return delegate.io();
    }

    @Override
    public EncryptionManager encryption() {
        return delegate.encryption();
    }

    @Override
    public LocationProvider locationProvider() {
        return delegate.locationProvider();
    }

    @Override
    public List<StatisticsFile> statisticsFiles() {
        return pinned != null ? pinned.statisticsFiles() : delegate.statisticsFiles();
    }

    @Override
    public List<PartitionStatisticsFile> partitionStatisticsFiles() {
        return pinned != null ? pinned.partitionStatisticsFiles() : delegate.partitionStatisticsFiles();
    }

    @Override
    public Map<String, SnapshotRef> refs() {
        return pinned != null ? pinned.refs() : delegate.refs();
    }

    @Override
    public UUID uuid() {
        if (pinned != null) {
            return UUID.fromString(pinned.uuid());
        }
        return delegate.uuid();
    }

    private TableScan configure(IcebergTableHandle handle, Schema selectedSchema) {
        // RESTTable.newScan() binds the scan to the SDK table. useRef therefore resolves refs there.
        TableScan scan = delegate.newScan();
        if (handle != null && handle.getRef() != null) {
            scan = scan.useRef(handle.getRef());
        } else if (handle != null && handle.getSnapshotId() >= 0) {
            scan = scan.useSnapshot(handle.getSnapshotId());
        } else {
            scan = pinSnapshot(scan);
        }
        if (selectedSchema != null && !scan.schema().sameSchema(selectedSchema)) {
            scan = scan.project(selectedSchema);
        }
        return scan;
    }

    private TableScan pinSnapshot(TableScan scan) {
        if (pinned == null) {
            return scan;
        }
        if (pinnedSnapshotId >= 0) {
            return scan.useSnapshot(pinnedSnapshotId);
        }
        Snapshot live = delegate.currentSnapshot();
        if (live != null) {
            // useSnapshot(-1) is illegal, and an unpinned REST scan would read the new commit.
            throw new DorisConnectorException("Iceberg server-planned read view for " + delegate.name()
                    + " was pinned with no snapshot, but the REST table now has snapshot "
                    + live.snapshotId());
        }
        return scan;
    }

    private static TableMetadata detach(Table delegate, TableMetadata metadata) {
        if (metadata == null) {
            return null;
        }
        // Serializing a lazily supplied snapshot list would fetch it. Share the loaded object instead.
        if (delegate instanceof HasTableOperations && !IcebergCacheSizeEstimator.areSnapshotsLoaded(delegate)) {
            return metadata;
        }
        String json = TableMetadataParser.toJson(metadata);
        return TableMetadataParser.fromJson(metadata.metadataFileLocation(), json);
    }

    private <T> T readOnly() {
        throw new UnsupportedOperationException(
                "Server-planned Iceberg read view is read-only: " + delegate.name());
    }
}
