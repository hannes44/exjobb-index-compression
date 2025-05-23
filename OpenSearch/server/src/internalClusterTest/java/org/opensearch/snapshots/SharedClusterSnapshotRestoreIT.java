/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.snapshots;

import org.apache.lucene.util.BytesRef;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStage;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.SnapshotsInProgress.State;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.util.BytesRefUtils;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.opensearch.index.shard.IndexShardTests.getEngineFromShard;
import static org.opensearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAllSuccessful;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertRequestBuilderThrows;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class SharedClusterSnapshotRestoreIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
            .build();
    }

    public void testBasicWorkFlow() throws Exception {
        createRepository("test-repo", "fs");
        createIndexWithRandomDocs("test-idx-1", 100);
        createIndexWithRandomDocs("test-idx-2", 100);
        createIndexWithRandomDocs("test-idx-3", 100);

        ActionFuture<FlushResponse> flushResponseFuture = null;
        if (randomBoolean()) {
            ArrayList<String> indicesToFlush = new ArrayList<>();
            for (int i = 1; i < 4; i++) {
                if (randomBoolean()) {
                    indicesToFlush.add("test-idx-" + i);
                }
            }
            if (!indicesToFlush.isEmpty()) {
                String[] indices = indicesToFlush.toArray(new String[indicesToFlush.size()]);
                logger.info("--> starting asynchronous flush for indices {}", Arrays.toString(indices));
                flushResponseFuture = client().admin().indices().prepareFlush(indices).execute();
            }
        }

        final String[] indicesToSnapshot = { "test-idx-*", "-test-idx-3" };

        logger.info("--> capturing history UUIDs");
        final Map<ShardId, String> historyUUIDs = new HashMap<>();
        for (ShardStats shardStats : client().admin().indices().prepareStats(indicesToSnapshot).clear().get().getShards()) {
            String historyUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);
            ShardId shardId = shardStats.getShardRouting().shardId();
            if (historyUUIDs.containsKey(shardId)) {
                assertThat(shardStats.getShardRouting() + " has a different history uuid", historyUUID, equalTo(historyUUIDs.get(shardId)));
            } else {
                historyUUIDs.put(shardId, historyUUID);
            }
        }

        final boolean snapshotClosed = randomBoolean();
        if (snapshotClosed) {
            assertAcked(client().admin().indices().prepareClose(indicesToSnapshot).setWaitForActiveShards(ActiveShardCount.ALL).get());
        }

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndicesOptions(IndicesOptions.lenientExpand())
            .setIndices(indicesToSnapshot)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo")
            .setSnapshots(randomFrom("test-snap", "_all", "*", "*-snap", "test*"))
            .get()
            .getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(Version.CURRENT));

        if (snapshotClosed) {
            assertAcked(client().admin().indices().prepareOpen(indicesToSnapshot).setWaitForActiveShards(ActiveShardCount.ALL).get());
        }

        logger.info("--> delete some data");
        for (int i = 0; i < 50; i++) {
            client().prepareDelete("test-idx-1", Integer.toString(i)).get();
        }
        for (int i = 50; i < 100; i++) {
            client().prepareDelete("test-idx-2", Integer.toString(i)).get();
        }
        for (int i = 0; i < 100; i += 2) {
            client().prepareDelete("test-idx-3", Integer.toString(i)).get();
        }
        assertAllSuccessful(refresh());
        assertDocCount("test-idx-1", 50L);
        assertDocCount("test-idx-2", 50L);
        assertDocCount("test-idx-3", 50L);

        logger.info("--> close indices");
        client().admin().indices().prepareClose("test-idx-1", "test-idx-2").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        assertDocCount("test-idx-1", 100L);
        assertDocCount("test-idx-2", 100L);
        assertDocCount("test-idx-3", 50L);

        assertNull(
            client().admin()
                .indices()
                .prepareGetSettings("test-idx-1")
                .get()
                .getSetting("test-idx-1", MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey())
        );

        for (ShardStats shardStats : client().admin().indices().prepareStats(indicesToSnapshot).clear().get().getShards()) {
            String historyUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);
            ShardId shardId = shardStats.getShardRouting().shardId();
            assertThat(shardStats.getShardRouting() + " doesn't have a history uuid", historyUUID, notNullValue());
            assertThat(shardStats.getShardRouting() + " doesn't have a new history", historyUUID, not(equalTo(historyUUIDs.get(shardId))));
        }

        // Test restore after index deletion
        logger.info("--> delete indices");
        cluster().wipeIndices("test-idx-1", "test-idx-2");
        logger.info("--> restore one index after deletion");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-2")
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        assertDocCount("test-idx-1", 100);
        ClusterState clusterState = clusterAdmin().prepareState().get().getState();
        assertThat(clusterState.getMetadata().hasIndex("test-idx-1"), equalTo(true));
        assertThat(clusterState.getMetadata().hasIndex("test-idx-2"), equalTo(false));

        assertNull(
            client().admin()
                .indices()
                .prepareGetSettings("test-idx-1")
                .get()
                .getSetting("test-idx-1", MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey())
        );

        for (ShardStats shardStats : client().admin().indices().prepareStats(indicesToSnapshot).clear().get().getShards()) {
            String historyUUID = shardStats.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY);
            ShardId shardId = shardStats.getShardRouting().shardId();
            assertThat(shardStats.getShardRouting() + " doesn't have a history uuid", historyUUID, notNullValue());
            assertThat(shardStats.getShardRouting() + " doesn't have a new history", historyUUID, not(equalTo(historyUUIDs.get(shardId))));
        }

        if (flushResponseFuture != null) {
            // Finish flush
            flushResponseFuture.actionGet();
        }
    }

    public void testSingleGetAfterRestore() throws Exception {
        String indexName = "testindex";
        String repoName = "test-restore-snapshot-repo";
        String snapshotName = "test-restore-snapshot";
        Path absolutePath = randomRepoPath().toAbsolutePath();
        logger.info("Path [{}]", absolutePath);
        String restoredIndexName = indexName + "-restored";
        String expectedValue = "expected";

        // Write a document
        String docId = Integer.toString(randomInt());
        index(indexName, MapperService.SINGLE_MAPPING_NAME, docId, "value", expectedValue);

        createRepository(repoName, "fs", absolutePath);
        createSnapshot(repoName, snapshotName, Collections.singletonList(indexName));

        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndexName)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertThat(client().prepareGet(restoredIndexName, docId).get().isExists(), equalTo(true));
    }

    public void testFreshIndexUUID() {
        createRepository("test-repo", "fs");

        createIndex("test");
        String originalIndexUUID = client().admin()
            .indices()
            .prepareGetSettings("test")
            .get()
            .getSetting("test", IndexMetadata.SETTING_INDEX_UUID);
        assertTrue(originalIndexUUID, originalIndexUUID != null);
        assertFalse(originalIndexUUID, originalIndexUUID.equals(IndexMetadata.INDEX_UUID_NA_VALUE));
        ensureGreen();
        createSnapshot("test-repo", "test-snap", Collections.singletonList("test"));
        NumShards numShards = getNumShards("test");

        cluster().wipeIndices("test");
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numShards.numPrimaries)));
        ensureGreen();
        String newIndexUUID = client().admin()
            .indices()
            .prepareGetSettings("test")
            .get()
            .getSetting("test", IndexMetadata.SETTING_INDEX_UUID);
        assertTrue(newIndexUUID, newIndexUUID != null);
        assertFalse(newIndexUUID, newIndexUUID.equals(IndexMetadata.INDEX_UUID_NA_VALUE));
        assertFalse(newIndexUUID, newIndexUUID.equals(originalIndexUUID));
        logger.info("--> close index");
        client().admin().indices().prepareClose("test").get();

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        ensureGreen();
        String newAfterRestoreIndexUUID = client().admin()
            .indices()
            .prepareGetSettings("test")
            .get()
            .getSetting("test", IndexMetadata.SETTING_INDEX_UUID);
        assertTrue(
            "UUID has changed after restore: " + newIndexUUID + " vs. " + newAfterRestoreIndexUUID,
            newIndexUUID.equals(newAfterRestoreIndexUUID)
        );

        logger.info("--> restore indices with different names");
        restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1-copy")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        String copyRestoreUUID = client().admin()
            .indices()
            .prepareGetSettings("test-copy")
            .get()
            .getSetting("test-copy", IndexMetadata.SETTING_INDEX_UUID);
        assertFalse(
            "UUID has been reused on restore: " + copyRestoreUUID + " vs. " + originalIndexUUID,
            copyRestoreUUID.equals(originalIndexUUID)
        );
    }

    public void testEmptySnapshot() throws Exception {
        createRepository("test-repo", "fs");

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = startFullSnapshot("test-repo", "test-snap").get();
        assertThat(createSnapshotResponse.getSnapshotInfo().totalShards(), equalTo(0));
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(0));

        assertThat(getSnapshot("test-repo", "test-snap").state(), equalTo(SnapshotState.SUCCESS));
    }

    public void testSnapshotFileFailureDuringSnapshot() throws InterruptedException {
        disableRepoConsistencyCheck("This test uses a purposely broken repository so it would fail consistency checks");

        logger.info("-->  creating repository");
        Settings.Builder settings = Settings.builder()
            .put("location", randomRepoPath())
            .put("random", randomAlphaOfLength(10))
            .put("random_control_io_exception_rate", 0.2);
        OpenSearchIntegTestCase.putRepository(clusterAdmin(), "test-repo", "mock", false, settings);

        createIndexWithRandomDocs("test-idx", 100);

        logger.info("--> snapshot");
        try {
            CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .setIndices("test-idx")
                .get();
            if (createSnapshotResponse.getSnapshotInfo().totalShards() == createSnapshotResponse.getSnapshotInfo().successfulShards()) {
                // If we are here, that means we didn't have any failures, let's check it
                assertThat(getFailureCount("test-repo"), equalTo(0L));
            } else {
                assertThat(getFailureCount("test-repo"), greaterThan(0L));
                assertThat(createSnapshotResponse.getSnapshotInfo().shardFailures().size(), greaterThan(0));
                for (SnapshotShardFailure shardFailure : createSnapshotResponse.getSnapshotInfo().shardFailures()) {
                    assertThat(shardFailure.reason(), containsString("Random IOException"));
                    assertThat(shardFailure.nodeId(), notNullValue());
                    assertThat(shardFailure.index(), equalTo("test-idx"));
                }
                SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
                if (snapshotInfo.state() == SnapshotState.SUCCESS) {
                    assertThat(snapshotInfo.shardFailures().size(), greaterThan(0));
                    assertThat(snapshotInfo.totalShards(), greaterThan(snapshotInfo.successfulShards()));
                }
            }
        } catch (Exception ex) {
            logger.info("--> caught a top level exception, asserting what's expected", ex);
            assertThat(getFailureCount("test-repo"), greaterThan(0L));

            final Throwable cause = ex.getCause();
            assertThat(cause, notNullValue());
            final Throwable rootCause = new OpenSearchException(cause).getRootCause();
            assertThat(rootCause, notNullValue());
            assertThat(rootCause.getMessage(), containsString("Random IOException"));
        }
    }

    public void testDataFileFailureDuringSnapshot() throws Exception {
        disableRepoConsistencyCheck("This test intentionally leaves a broken repository");

        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                .put("random", randomAlphaOfLength(10))
                .put("random_data_file_io_exception_rate", 0.3)
        );

        createIndexWithRandomDocs("test-idx", 100);

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .get();
        if (createSnapshotResponse.getSnapshotInfo().totalShards() == createSnapshotResponse.getSnapshotInfo().successfulShards()) {
            logger.info("--> no failures");
            // If we are here, that means we didn't have any failures, let's check it
            assertThat(getFailureCount("test-repo"), equalTo(0L));
        } else {
            logger.info("--> some failures");
            assertThat(getFailureCount("test-repo"), greaterThan(0L));
            assertThat(createSnapshotResponse.getSnapshotInfo().shardFailures().size(), greaterThan(0));
            for (SnapshotShardFailure shardFailure : createSnapshotResponse.getSnapshotInfo().shardFailures()) {
                assertThat(shardFailure.nodeId(), notNullValue());
                assertThat(shardFailure.index(), equalTo("test-idx"));
            }
            SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
            assertThat(snapshotInfo.state(), equalTo(SnapshotState.PARTIAL));
            assertThat(snapshotInfo.shardFailures().size(), greaterThan(0));
            assertThat(snapshotInfo.totalShards(), greaterThan(snapshotInfo.successfulShards()));

            // Verify that snapshot status also contains the same failures
            SnapshotsStatusResponse snapshotsStatusResponse = clusterAdmin().prepareSnapshotStatus("test-repo")
                .addSnapshots("test-snap")
                .get();
            assertThat(snapshotsStatusResponse.getSnapshots().size(), equalTo(1));
            SnapshotStatus snapshotStatus = snapshotsStatusResponse.getSnapshots().get(0);
            assertThat(snapshotStatus.getIndices().size(), equalTo(1));
            SnapshotIndexStatus indexStatus = snapshotStatus.getIndices().get("test-idx");
            assertThat(indexStatus, notNullValue());
            assertThat(indexStatus.getShardsStats().getFailedShards(), equalTo(snapshotInfo.failedShards()));
            assertThat(indexStatus.getShardsStats().getDoneShards(), equalTo(snapshotInfo.successfulShards()));
            assertThat(indexStatus.getShards().size(), equalTo(snapshotInfo.totalShards()));

            int numberOfFailures = 0;
            for (SnapshotIndexShardStatus shardStatus : indexStatus.getShards().values()) {
                if (shardStatus.getStage() == SnapshotIndexShardStage.FAILURE) {
                    assertThat(shardStatus.getFailure(), notNullValue());
                    numberOfFailures++;
                } else {
                    assertThat(shardStatus.getFailure(), nullValue());
                }
            }
            assertThat(indexStatus.getShardsStats().getFailedShards(), equalTo(numberOfFailures));
        }
    }

    public void testDataFileFailureDuringRestore() throws Exception {
        disableRepoConsistencyCheck("This test intentionally leaves a broken repository");

        Path repositoryLocation = randomRepoPath();
        Client client = client();
        createRepository("test-repo", "fs", repositoryLocation);

        prepareCreate("test-idx").setSettings(Settings.builder().put("index.allocation.max_retries", Integer.MAX_VALUE)).get();
        ensureGreen();

        final NumShards numShards = getNumShards("test-idx");

        indexRandomDocs("test-idx", 100);

        createSnapshot("test-repo", "test-snap", Collections.singletonList("test-idx"));

        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", repositoryLocation)
                .put("random", randomAlphaOfLength(10))
                .put("random_data_file_io_exception_rate", 0.3)
        );

        // Test restore after index deletion
        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> restore index after deletion");
        final RestoreSnapshotResponse restoreResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .get();

        logger.info("--> total number of simulated failures during restore: [{}]", getFailureCount("test-repo"));
        final RestoreInfo restoreInfo = restoreResponse.getRestoreInfo();
        assertThat(restoreInfo.totalShards(), equalTo(numShards.numPrimaries));

        if (restoreInfo.successfulShards() == restoreInfo.totalShards()) {
            // All shards were restored, we must find the exact number of hits
            assertDocCount("test-idx", 100L);
        } else {
            // One or more shards failed to be restored. This can happen when there is
            // only 1 data node: a shard failed because of the random IO exceptions
            // during restore and then we don't allow the shard to be assigned on the
            // same node again during the same reroute operation. Then another reroute
            // operation is scheduled, but the RestoreInProgressAllocationDecider will
            // block the shard to be assigned again because it failed during restore.
            final ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().get();
            assertEquals(1, clusterStateResponse.getState().getNodes().getDataNodes().size());
            assertEquals(
                restoreInfo.failedShards(),
                clusterStateResponse.getState().getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED).size()
            );
        }
    }

    public void testDataFileCorruptionDuringRestore() throws Exception {
        disableRepoConsistencyCheck("This test intentionally leaves a broken repository");

        Path repositoryLocation = randomRepoPath();
        Client client = client();

        createRepository("test-repo", "fs", repositoryLocation);
        createIndexWithRandomDocs("test-idx", 100);

        createSnapshot("test-repo", "test-snap", Collections.singletonList("test-idx"));

        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", repositoryLocation)
                .put("random", randomAlphaOfLength(10))
                .put("use_lucene_corruption", true)
                .put("max_failure_number", 10000000L)
                .put("random_data_file_io_exception_rate", 1.0)
        );

        // Test restore after index deletion
        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> restore corrupt index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setClusterManagerNodeTimeout("30s")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(
            restoreSnapshotResponse.getRestoreInfo().failedShards(),
            equalTo(restoreSnapshotResponse.getRestoreInfo().totalShards())
        );
        // we have to delete the index here manually, otherwise the cluster will keep
        // trying to allocate the shards for the index, even though the restore operation
        // is completed and marked as failed, which can lead to nodes having pending
        // cluster states to process in their queue when the test is finished
        cluster().wipeIndices("test-idx");
    }

    /**
     * Test that restoring a snapshot whose files can't be downloaded at all is not stuck or
     * does not hang indefinitely.
     */
    public void testUnrestorableFilesDuringRestore() throws Exception {
        final String indexName = "unrestorable-files";
        final int maxRetries = randomIntBetween(1, 10);

        Settings createIndexSettings = Settings.builder()
            .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), maxRetries)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .build();

        Settings repositorySettings = Settings.builder()
            .put("random", randomAlphaOfLength(10))
            .put("max_failure_number", 10000000L)
            // No lucene corruptions, we want to test retries
            .put("use_lucene_corruption", false)
            // Restoring a file will never complete
            .put("random_data_file_io_exception_rate", 1.0)
            .build();

        Consumer<UnassignedInfo> checkUnassignedInfo = unassignedInfo -> {
            assertThat(unassignedInfo.getReason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
            assertThat(unassignedInfo.getNumFailedAllocations(), anyOf(equalTo(maxRetries), equalTo(1)));
        };

        unrestorableUseCase(indexName, createIndexSettings, repositorySettings, Settings.EMPTY, checkUnassignedInfo, () -> {});
    }

    /**
     * Test that restoring an index with shard allocation filtering settings that prevents
     * its allocation does not hang indefinitely.
     */
    public void testUnrestorableIndexDuringRestore() throws Exception {
        final String indexName = "unrestorable-index";
        Settings restoreIndexSettings = Settings.builder().put("index.routing.allocation.include._name", randomAlphaOfLength(5)).build();

        Runnable fixupAction = () -> {
            // remove the shard allocation filtering settings and use the Reroute API to retry the failed shards
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().putNull("index.routing.allocation.include._name").build())
            );
            assertAcked(clusterAdmin().prepareReroute().setRetryFailed(true));
        };

        unrestorableUseCase(
            indexName,
            Settings.EMPTY,
            Settings.EMPTY,
            restoreIndexSettings,
            unassignedInfo -> assertThat(unassignedInfo.getReason(), equalTo(UnassignedInfo.Reason.NEW_INDEX_RESTORED)),
            fixupAction
        );
    }

    /** Execute the unrestorable test use case **/
    private void unrestorableUseCase(
        final String indexName,
        final Settings createIndexSettings,
        final Settings repositorySettings,
        final Settings restoreIndexSettings,
        final Consumer<UnassignedInfo> checkUnassignedInfo,
        final Runnable fixUpAction
    ) throws Exception {
        // create a test repository
        final Path repositoryLocation = randomRepoPath();
        createRepository("test-repo", "fs", repositoryLocation);
        // create a test index
        assertAcked(prepareCreate(indexName, Settings.builder().put(createIndexSettings)));

        // index some documents
        final int nbDocs = scaledRandomIntBetween(10, 100);
        indexRandomDocs(indexName, nbDocs);

        // create a snapshot
        final NumShards numShards = getNumShards(indexName);
        final SnapshotInfo snapshotInfo = createSnapshot("test-repo", "test-snap", Collections.singletonList(indexName));
        assertThat(snapshotInfo.successfulShards(), equalTo(numShards.numPrimaries));

        // delete the test index
        assertAcked(client().admin().indices().prepareDelete(indexName));

        // update the test repository
        Settings.Builder settings = Settings.builder().put("location", repositoryLocation).put(repositorySettings);
        OpenSearchIntegTestCase.putRepository(clusterAdmin(), "test-repo", "mock", settings);

        // attempt to restore the snapshot with the given settings
        RestoreSnapshotResponse restoreResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setIndices(indexName)
            .setIndexSettings(restoreIndexSettings)
            .setWaitForCompletion(true)
            .get();

        // check that all shards failed during restore
        assertThat(restoreResponse.getRestoreInfo().totalShards(), equalTo(numShards.numPrimaries));
        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(0));

        ClusterStateResponse clusterStateResponse = clusterAdmin().prepareState().setCustoms(true).setRoutingTable(true).get();

        // check that there is no restore in progress
        RestoreInProgress restoreInProgress = clusterStateResponse.getState().custom(RestoreInProgress.TYPE);
        assertNotNull("RestoreInProgress must be not null", restoreInProgress);
        assertTrue("RestoreInProgress must be empty but found entries in " + restoreInProgress, restoreInProgress.isEmpty());

        // check that the shards have been created but are not assigned
        assertThat(clusterStateResponse.getState().getRoutingTable().allShards(indexName), hasSize(numShards.totalNumShards));

        // check that every primary shard is unassigned
        for (ShardRouting shard : clusterStateResponse.getState().getRoutingTable().allShards(indexName)) {
            if (shard.primary()) {
                assertThat(shard.state(), equalTo(ShardRoutingState.UNASSIGNED));
                assertThat(shard.recoverySource().getType(), equalTo(RecoverySource.Type.SNAPSHOT));
                assertThat(shard.unassignedInfo().getLastAllocationStatus(), equalTo(UnassignedInfo.AllocationStatus.DECIDERS_NO));
                checkUnassignedInfo.accept(shard.unassignedInfo());
            }
        }

        // update the test repository in order to make it work
        createRepository("test-repo", "fs", repositoryLocation);

        // execute action to eventually fix the situation
        fixUpAction.run();

        // delete the index and restore again
        assertAcked(client().admin().indices().prepareDelete(indexName));

        restoreResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).get();
        assertThat(restoreResponse.getRestoreInfo().totalShards(), equalTo(numShards.numPrimaries));
        assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(numShards.numPrimaries));

        // Wait for the shards to be assigned
        ensureGreen(indexName);
        refresh(indexName);

        assertDocCount(indexName, nbDocs);
    }

    public void testDeletionOfFailingToRecoverIndexShouldStopRestore() throws Exception {
        Path repositoryLocation = randomRepoPath();
        Client client = client();
        createRepository("test-repo", "fs", repositoryLocation);

        createIndexWithRandomDocs("test-idx", 100);

        createSnapshot("test-repo", "test-snap", Collections.singletonList("test-idx"));

        logger.info("-->  update repository with mock version");
        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", repositoryLocation)
                .put("random", randomAlphaOfLength(10))
                .put("random_data_file_io_exception_rate", 1.0) // Fail completely
        );

        // Test restore after index deletion
        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> restore index after deletion");
        ActionFuture<RestoreSnapshotResponse> restoreSnapshotResponseFuture = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute();

        logger.info("--> wait for the index to appear");
        // that would mean that recovery process started and failing
        waitForIndex("test-idx", TimeValue.timeValueSeconds(10));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");
        logger.info("--> get restore results");
        // Now read restore results and make sure it failed
        RestoreSnapshotResponse restoreSnapshotResponse = restoreSnapshotResponseFuture.actionGet(TimeValue.timeValueSeconds(10));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), greaterThan(0));
        assertThat(
            restoreSnapshotResponse.getRestoreInfo().totalShards(),
            equalTo(restoreSnapshotResponse.getRestoreInfo().failedShards())
        );

        logger.info("--> restoring working repository");
        createRepository("test-repo", "fs", repositoryLocation);

        logger.info("--> trying to restore index again");
        restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertDocCount("test-idx", 100L);
    }

    public void testUnallocatedShards() {
        disableRepoConsistencyCheck("This test intentionally leaves an empty repository");
        createRepository("test-repo", "fs");

        logger.info("-->  creating index that cannot be allocated");
        prepareCreate(
            "test-idx",
            2,
            Settings.builder()
                .put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + "tag", "nowhere")
                .put("index.number_of_shards", 3)
        ).setWaitForActiveShards(ActiveShardCount.NONE).get();

        logger.info("--> snapshot");
        final SnapshotException sne = expectThrows(
            SnapshotException.class,
            () -> clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(true).setIndices("test-idx").get()
        );
        assertThat(sne.getMessage(), containsString("Indices don't have primary shards"));
        assertThat(getRepositoryData("test-repo"), is(RepositoryData.EMPTY));
    }

    public void testDeleteSnapshot() throws Exception {
        final int numberOfSnapshots = between(5, 15);
        Client client = client();

        Path repo = randomRepoPath();
        createRepository(
            "test-repo",
            "fs",
            Settings.builder()
                .put("location", repo)
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx");
        ensureGreen();

        int[] numberOfFiles = new int[numberOfSnapshots];
        logger.info("--> creating {} snapshots ", numberOfSnapshots);
        for (int i = 0; i < numberOfSnapshots; i++) {
            for (int j = 0; j < 10; j++) {
                index("test-idx", "_doc", Integer.toString(i * 10 + j), "foo", "bar" + i * 10 + j);
            }
            refresh();
            createSnapshot("test-repo", "test-snap-" + i, Collections.singletonList("test-idx"));
            // Store number of files after each snapshot
            numberOfFiles[i] = numberOfFiles(repo);
        }
        assertDocCount("test-idx", 10L * numberOfSnapshots);
        int numberOfFilesBeforeDeletion = numberOfFiles(repo);

        logger.info("--> delete all snapshots except the first one and last one");

        if (randomBoolean()) {
            for (int i = 1; i < numberOfSnapshots - 1; i++) {
                client.admin().cluster().prepareDeleteSnapshot("test-repo", new String[] { "test-snap-" + i }).get();
            }
        } else {
            client.admin()
                .cluster()
                .prepareDeleteSnapshot(
                    "test-repo",
                    IntStream.range(1, numberOfSnapshots - 1).mapToObj(i -> "test-snap-" + i).toArray(String[]::new)
                )
                .get();
        }

        int numberOfFilesAfterDeletion = numberOfFiles(repo);

        assertThat(numberOfFilesAfterDeletion, lessThan(numberOfFilesBeforeDeletion));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> restore index");
        String lastSnapshot = "test-snap-" + (numberOfSnapshots - 1);
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", lastSnapshot)
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertDocCount("test-idx", 10L * numberOfSnapshots);

        startDeleteSnapshot("test-repo", lastSnapshot).get();
        logger.info("--> make sure that number of files is back to what it was when the first snapshot was made");
        assertFileCount(repo, numberOfFiles[0]);
    }

    public void testSnapshotClosedIndex() throws Exception {
        Client client = client();

        createRepository("test-repo", "fs");

        createIndex("test-idx", "test-idx-closed");
        ensureGreen();
        logger.info("-->  closing index test-idx-closed");
        assertAcked(client.admin().indices().prepareClose("test-idx-closed").setWaitForActiveShards(ActiveShardCount.ALL));
        ClusterStateResponse stateResponse = client.admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test-idx-closed").getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test-idx-closed"), notNullValue());

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx*")
            .setIndicesOptions(IndicesOptions.lenientExpand())
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().indices().size(), equalTo(2));
        assertThat(createSnapshotResponse.getSnapshotInfo().shardFailures().size(), equalTo(0));
    }

    public void testMoveShardWhileSnapshotting() throws Exception {
        Client client = client();
        Path repositoryLocation = randomRepoPath();
        createRepository(
            "test-repo",
            "mock",
            Settings.builder().put("location", repositoryLocation).put("random", randomAlphaOfLength(10)).put("wait_after_unblock", 200)
        );

        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_replicas", 0)));
        indexRandomDocs("test-idx", 100);

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], moving shards away from this node", blockedNode);
        Settings.Builder excludeSettings = Settings.builder().put("index.routing.allocation.exclude._name", blockedNode);
        client().admin().indices().prepareUpdateSettings("test-idx").setSettings(excludeSettings).get();

        logger.info("--> unblocking blocked node");
        unblockNode("test-repo", blockedNode);
        logger.info("--> waiting for completion");
        logger.info(
            "Number of failed shards [{}]",
            waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(600)).shardFailures().size()
        );
        logger.info("--> done");

        final SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.shardFailures(), empty());

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> replace mock repository with real one at the same location");
        createRepository("test-repo", "fs", repositoryLocation);

        logger.info("--> restore index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertDocCount("test-idx", 100L);
    }

    public void testDeleteRepositoryWhileSnapshotting() throws Exception {
        disableRepoConsistencyCheck("This test uses a purposely broken repository so it would fail consistency checks");
        Client client = client();
        Path repositoryLocation = randomRepoPath();
        createRepository(
            "test-repo",
            "mock",
            Settings.builder().put("location", repositoryLocation).put("random", randomAlphaOfLength(10)).put("wait_after_unblock", 200)
        );

        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_replicas", 0)));

        indexRandomDocs("test-idx", 100);

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], trying to delete repository", blockedNode);

        try {
            client.admin().cluster().prepareDeleteRepository(randomFrom("test-repo", "test-*", "*")).execute().actionGet();
            fail("shouldn't be able to delete in-use repository");
        } catch (Exception ex) {
            logger.info("--> in-use repository deletion failed");
            assertThat(ex.getMessage(), containsString("trying to modify or unregister repository that is currently used"));
        }

        logger.info("--> trying to move repository to another location");
        Settings.Builder settings = Settings.builder().put("location", repositoryLocation.resolve("test"));
        try {
            OpenSearchIntegTestCase.putRepository(client.admin().cluster(), "test-repo", "fs", settings);
            fail("shouldn't be able to replace in-use repository");
        } catch (Exception ex) {
            logger.info("--> in-use repository replacement failed");
        }

        logger.info("--> trying to create a repository with different name");
        Settings.Builder settingsBuilder = Settings.builder().put("location", repositoryLocation.resolve("test"));
        OpenSearchIntegTestCase.putRepository(client.admin().cluster(), "test-repo-2", "fs", false, settingsBuilder);

        logger.info("--> unblocking blocked node");
        unblockNode("test-repo", blockedNode);
        logger.info("--> waiting for completion");
        logger.info(
            "Number of failed shards [{}]",
            waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(600)).shardFailures().size()
        );
        logger.info("--> done");

        final SnapshotInfo snapshotInfo = getSnapshot("test-repo", "test-snap");
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.shardFailures().size(), equalTo(0));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> replace mock repository with real one at the same location");
        createRepository("test-repo", "fs", repositoryLocation);

        logger.info("--> restore index");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertDocCount("test-idx", 100);
    }

    public void testReadonlyRepository() throws Exception {
        Client client = client();
        Path repositoryLocation = randomRepoPath();
        createRepository("test-repo", "fs", repositoryLocation);

        createIndexWithRandomDocs("test-idx", 100);

        createSnapshot("test-repo", "test-snap", Collections.singletonList("test-idx"));

        assertThat(getSnapshot("test-repo", "test-snap").state(), equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        createRepository(
            "readonly-repo",
            "fs",
            Settings.builder()
                .put("location", repositoryLocation)
                .put("compress", randomBoolean())
                .put("readonly", true)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );
        logger.info("--> restore index after deletion");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("readonly-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertDocCount("test-idx", 100L);

        logger.info("--> list available shapshots");
        GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots("readonly-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots(), notNullValue());
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(1));

        logger.info("--> try deleting snapshot");
        assertRequestBuilderThrows(
            client.admin().cluster().prepareDeleteSnapshot("readonly-repo", "test-snap"),
            RepositoryException.class,
            "cannot delete snapshot from a readonly repository"
        );

        logger.info("--> try making another snapshot");
        assertRequestBuilderThrows(
            client.admin()
                .cluster()
                .prepareCreateSnapshot("readonly-repo", "test-snap-2")
                .setWaitForCompletion(true)
                .setIndices("test-idx"),
            RepositoryException.class,
            "cannot create snapshot in a readonly repository"
        );
    }

    public void testThrottling() throws Exception {
        Client client = client();

        boolean throttleSnapshot = randomBoolean();
        boolean throttleRestore = randomBoolean();
        boolean throttleRestoreViaRecoverySettings = throttleRestore && randomBoolean();
        createRepository(
            "test-repo",
            "fs",
            Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(1000, 10000), ByteSizeUnit.BYTES)
                .put("max_restore_bytes_per_sec", throttleRestore && (throttleRestoreViaRecoverySettings == false) ? "10k" : "0")
                .put("max_snapshot_bytes_per_sec", throttleSnapshot ? "10k" : "0")
        );

        createIndexWithRandomDocs("test-idx", 100);
        createSnapshot("test-repo", "test-snap", Collections.singletonList("test-idx"));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> restore index");
        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), throttleRestoreViaRecoverySettings ? "10k" : "0")
                    .build()
            )
            .get();
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertDocCount("test-idx", 100L);

        long snapshotPause = 0L;
        long restorePause = 0L;
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            snapshotPause += repositoriesService.repository("test-repo").getSnapshotThrottleTimeInNanos();
            restorePause += repositoriesService.repository("test-repo").getRestoreThrottleTimeInNanos();
        }

        if (throttleSnapshot) {
            assertThat(snapshotPause, greaterThan(0L));
        } else {
            assertThat(snapshotPause, equalTo(0L));
        }

        if (throttleRestore) {
            assertThat(restorePause, greaterThan(0L));
        } else {
            assertThat(restorePause, equalTo(0L));
        }
        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()).build())
            .get();
    }

    public void testSnapshotStatus() throws Exception {
        Client client = client();
        createRepository(
            "test-repo",
            "mock",
            Settings.builder().put("location", randomRepoPath()).put("random", randomAlphaOfLength(10)).put("wait_after_unblock", 200)
        );

        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate("test-idx", 2, Settings.builder().put("number_of_replicas", 0)));
        indexRandomDocs("test-idx", 100);

        // Pick one node and block it
        String blockedNode = blockNodeWithIndex("test-repo", "test-idx");

        logger.info("--> snapshot");
        client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(false)
            .setIncludeGlobalState(false)
            .setIndices("test-idx")
            .get();

        logger.info("--> waiting for block to kick in");
        waitForBlock(blockedNode, "test-repo", TimeValue.timeValueSeconds(60));

        logger.info("--> execution was blocked on node [{}], checking snapshot status with specified repository and snapshot", blockedNode);
        SnapshotsStatusResponse response = client.admin().cluster().prepareSnapshotStatus("test-repo").execute().actionGet();
        assertThat(response.getSnapshots().size(), equalTo(1));
        SnapshotStatus snapshotStatus = response.getSnapshots().get(0);
        assertThat(snapshotStatus.getState(), equalTo(State.STARTED));
        assertThat(snapshotStatus.includeGlobalState(), equalTo(false));

        // We blocked the node during data write operation, so at least one shard snapshot should be in STARTED stage
        assertThat(snapshotStatus.getShardsStats().getStartedShards(), greaterThan(0));
        for (SnapshotIndexShardStatus shardStatus : snapshotStatus.getIndices().get("test-idx")) {
            if (shardStatus.getStage() == SnapshotIndexShardStage.STARTED) {
                assertThat(shardStatus.getNodeId(), notNullValue());
            }
        }

        logger.info("--> checking snapshot status for all currently running and snapshot with empty repository");
        response = client.admin().cluster().prepareSnapshotStatus().execute().actionGet();
        assertThat(response.getSnapshots().size(), equalTo(1));
        snapshotStatus = response.getSnapshots().get(0);
        assertThat(snapshotStatus.getState(), equalTo(State.STARTED));
        assertThat(snapshotStatus.includeGlobalState(), equalTo(false));

        // We blocked the node during data write operation, so at least one shard snapshot should be in STARTED stage
        assertThat(snapshotStatus.getShardsStats().getStartedShards(), greaterThan(0));
        for (SnapshotIndexShardStatus shardStatus : snapshotStatus.getIndices().get("test-idx")) {
            if (shardStatus.getStage() == SnapshotIndexShardStage.STARTED) {
                assertThat(shardStatus.getNodeId(), notNullValue());
            }
        }

        logger.info("--> checking that _current returns the currently running snapshot");
        GetSnapshotsResponse getResponse = client.admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setCurrentSnapshot()
            .execute()
            .actionGet();
        assertThat(getResponse.getSnapshots().size(), equalTo(1));
        SnapshotInfo snapshotInfo = getResponse.getSnapshots().get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.IN_PROGRESS));

        logger.info("--> unblocking blocked node");
        unblockNode("test-repo", blockedNode);

        snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(600));
        logger.info("Number of failed shards [{}]", snapshotInfo.shardFailures().size());
        logger.info("--> done");

        logger.info("--> checking snapshot status again after snapshot is done");
        response = client.admin().cluster().prepareSnapshotStatus("test-repo").addSnapshots("test-snap").execute().actionGet();
        snapshotStatus = response.getSnapshots().get(0);
        assertThat(snapshotStatus.getIndices().size(), equalTo(1));
        assertThat(snapshotStatus.includeGlobalState(), equalTo(false));

        SnapshotIndexStatus indexStatus = snapshotStatus.getIndices().get("test-idx");
        assertThat(indexStatus, notNullValue());
        assertThat(indexStatus.getShardsStats().getInitializingShards(), equalTo(0));
        assertThat(indexStatus.getShardsStats().getFailedShards(), equalTo(snapshotInfo.failedShards()));
        assertThat(indexStatus.getShardsStats().getDoneShards(), equalTo(snapshotInfo.successfulShards()));
        assertThat(indexStatus.getShards().size(), equalTo(snapshotInfo.totalShards()));

        logger.info("--> checking snapshot status after it is done with empty repository");
        response = client.admin().cluster().prepareSnapshotStatus().execute().actionGet();
        assertThat(response.getSnapshots().size(), equalTo(0));

        logger.info("--> checking that _current no longer returns the snapshot");
        assertThat(
            client.admin()
                .cluster()
                .prepareGetSnapshots("test-repo")
                .addSnapshots("_current")
                .execute()
                .actionGet()
                .getSnapshots()
                .isEmpty(),
            equalTo(true)
        );

        // test that getting an unavailable snapshot status throws an exception if ignoreUnavailable is false on the request
        SnapshotMissingException ex = expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareSnapshotStatus("test-repo").addSnapshots("test-snap-doesnt-exist").get()
        );
        assertEquals("[test-repo:test-snap-doesnt-exist] is missing", ex.getMessage());
        // test that getting an unavailable snapshot status does not throw an exception if ignoreUnavailable is true on the request
        response = client.admin()
            .cluster()
            .prepareSnapshotStatus("test-repo")
            .addSnapshots("test-snap-doesnt-exist")
            .setIgnoreUnavailable(true)
            .get();
        assertTrue(response.getSnapshots().isEmpty());
        // test getting snapshot status for available and unavailable snapshots where ignoreUnavailable is true
        // (available one should be returned)
        response = client.admin()
            .cluster()
            .prepareSnapshotStatus("test-repo")
            .addSnapshots("test-snap", "test-snap-doesnt-exist")
            .setIgnoreUnavailable(true)
            .get();
        assertEquals(1, response.getSnapshots().size());
        assertEquals("test-snap", response.getSnapshots().get(0).getSnapshot().getSnapshotId().getName());
    }

    public void testSnapshotRelocatingPrimary() throws Exception {
        Client client = client();
        createRepository("test-repo", "fs");

        // Create index on two nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate("test-idx", 2, indexSettingsNoReplicas(between(2, 10))));

        ensureGreen("test-idx");
        indexRandomDocs("test-idx", 100);

        logger.info("--> start relocations");
        allowNodes("test-idx", 1);

        logger.info("--> wait for relocations to start");

        assertBusy(
            () -> assertThat(clusterAdmin().prepareHealth("test-idx").execute().actionGet().getRelocatingShards(), greaterThan(0)),
            1L,
            TimeUnit.MINUTES
        );

        logger.info("--> snapshot");
        client.admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setWaitForCompletion(false).setIndices("test-idx").get();

        logger.info("--> wait for snapshot to complete");
        SnapshotInfo snapshotInfo = waitForCompletion("test-repo", "test-snap", TimeValue.timeValueSeconds(600));
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.shardFailures().size(), equalTo(0));
        logger.info("--> done");
    }

    public void testSnapshotMoreThanOnce() throws InterruptedException {
        Client client = client();

        createRepository("test-repo", "fs");

        // only one shard
        final Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
        assertAcked(prepareCreate("test").setSettings(indexSettings));
        ensureGreen();

        indexRandomDocs("test", randomIntBetween(10, 100));
        assertNoFailures(client().admin().indices().prepareForceMerge("test").setFlush(true).setMaxNumSegments(1).get());

        createSnapshot("test-repo", "test", Collections.singletonList("test"));
        assertThat(getSnapshot("test-repo", "test").state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client.admin()
                .cluster()
                .prepareSnapshotStatus("test-repo")
                .setSnapshots("test")
                .get()
                .getSnapshots()
                .get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFileCount(), greaterThan(1));
            }
        }

        createSnapshot("test-repo", "test-1", Collections.singletonList("test"));
        assertThat(getSnapshot("test-repo", "test-1").state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client.admin()
                .cluster()
                .prepareSnapshotStatus("test-repo")
                .setSnapshots("test-1")
                .get()
                .getSnapshots()
                .get(0);
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                assertThat(status.getStats().getProcessedFileCount(), equalTo(0));
            }
        }

        client().prepareDelete("test", "1").get();
        createSnapshot("test-repo", "test-2", Collections.singletonList("test"));
        assertThat(getSnapshot("test-repo", "test-2").state(), equalTo(SnapshotState.SUCCESS));
        {
            SnapshotStatus snapshotStatus = client.admin()
                .cluster()
                .prepareSnapshotStatus("test-repo")
                .setSnapshots("test-2")
                .get()
                .getSnapshots()
                .get(0);
            Settings settings = client.admin().indices().prepareGetSettings("test").get().getIndexToSettings().get("test");
            List<SnapshotIndexShardStatus> shards = snapshotStatus.getShards();
            for (SnapshotIndexShardStatus status : shards) {
                // we flush before the snapshot such that we have to process the segments_N files plus the .del file
                // soft-delete generates DV files.
                assertThat(status.getStats().getProcessedFileCount(), greaterThan(2));
            }
        }
    }

    public void testCloseOrDeleteIndexDuringSnapshot() throws Exception {
        disableRepoConsistencyCheck("This test intentionally leaves a broken repository");

        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                .put("block_on_data", true)
        );
        createIndexWithRandomDocs("test-idx-1", 100);
        createIndexWithRandomDocs("test-idx-2", 100);
        createIndexWithRandomDocs("test-idx-3", 100);

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> future = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap")
            .setIndices("test-idx-*")
            .setWaitForCompletion(true)
            .setPartial(false)
            .execute();
        logger.info("--> wait for block to kick in");
        waitForBlockOnAnyDataNode("test-repo", TimeValue.timeValueMinutes(1));

        try {
            // non-partial snapshots do not allow close / delete operations on indices where snapshot has not been completed
            if (randomBoolean()) {
                try {
                    logger.info("--> delete index while non-partial snapshot is running");
                    client().admin().indices().prepareDelete("test-idx-1").get();
                    fail("Expected deleting index to fail during snapshot");
                } catch (SnapshotInProgressException e) {
                    assertThat(e.getMessage(), containsString("Cannot delete indices that are being snapshotted: [[test-idx-1/"));
                }
            } else {
                try {
                    logger.info("--> close index while non-partial snapshot is running");
                    client().admin().indices().prepareClose("test-idx-1").get();
                    fail("Expected closing index to fail during snapshot");
                } catch (SnapshotInProgressException e) {
                    assertThat(e.getMessage(), containsString("Cannot close indices that are being snapshotted: [[test-idx-1/"));
                }
            }
        } finally {
            logger.info("--> unblock all data nodes");
            unblockAllDataNodes("test-repo");
        }
        logger.info("--> waiting for snapshot to finish");
        CreateSnapshotResponse createSnapshotResponse = future.get();

        logger.info("Snapshot successfully completed");
        assertThat(createSnapshotResponse.getSnapshotInfo().state(), equalTo((SnapshotState.SUCCESS)));
    }

    public void testCloseIndexDuringRestore() throws Exception {
        Client client = client();

        createRepository("test-repo", "mock");

        createIndexWithRandomDocs("test-idx-1", 100);
        createIndexWithRandomDocs("test-idx-2", 100);

        logger.info("--> snapshot");
        assertThat(
            client.admin()
                .cluster()
                .prepareCreateSnapshot("test-repo", "test-snap")
                .setIndices("test-idx-*")
                .setWaitForCompletion(true)
                .get()
                .getSnapshotInfo()
                .state(),
            equalTo(SnapshotState.SUCCESS)
        );

        logger.info("--> deleting indices before restoring");
        assertAcked(client.admin().indices().prepareDelete("test-idx-*").get());

        blockAllDataNodes("test-repo");
        logger.info("--> execution will be blocked on all data nodes");

        final ActionFuture<RestoreSnapshotResponse> restoreFut;
        try {
            logger.info("--> start restore");
            restoreFut = client.admin().cluster().prepareRestoreSnapshot("test-repo", "test-snap").setWaitForCompletion(true).execute();

            logger.info("--> waiting for block to kick in");
            waitForBlockOnAnyDataNode("test-repo", TimeValue.timeValueMinutes(1));

            logger.info("--> close index while restore is running");
            try {
                client.admin().indices().prepareClose("test-idx-1").get();
                fail("Expected closing index to fail during restore");
            } catch (IllegalArgumentException e) {
                assertThat(e.getMessage(), containsString("Cannot close indices that are being restored: [[test-idx-1/"));
            }
        } finally {
            // unblock even if the try block fails otherwise we will get bogus failures when we delete all indices in test teardown.
            logger.info("--> unblocking all data nodes");
            unblockAllDataNodes("test-repo");
        }

        logger.info("--> wait for restore to finish");
        RestoreSnapshotResponse restoreSnapshotResponse = restoreFut.get();
        logger.info("--> check that all shards were recovered");
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), greaterThan(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
    }

    public void testDeleteSnapshotWhileRestoringFails() throws Exception {
        Client client = client();

        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        logger.info("--> creating index");
        final String indexName = "test-idx";
        assertAcked(prepareCreate(indexName).setWaitForActiveShards(ActiveShardCount.ALL));
        indexRandomDocs(indexName, 100);

        logger.info("--> take snapshots");
        final String snapshotName = "test-snap";
        assertThat(
            client.admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName)
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .get()
                .getSnapshotInfo()
                .state(),
            equalTo(SnapshotState.SUCCESS)
        );
        final String snapshotName2 = "test-snap-2";
        assertThat(
            client.admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapshotName2)
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .get()
                .getSnapshotInfo()
                .state(),
            equalTo(SnapshotState.SUCCESS)
        );

        logger.info("--> delete index before restoring");
        assertAcked(client.admin().indices().prepareDelete(indexName).get());

        logger.info("--> execution will be blocked on all data nodes");
        blockAllDataNodes(repoName);

        final ActionFuture<RestoreSnapshotResponse> restoreFut;
        try {
            logger.info("--> start restore");
            restoreFut = client.admin().cluster().prepareRestoreSnapshot(repoName, snapshotName).setWaitForCompletion(true).execute();

            logger.info("--> waiting for block to kick in");
            waitForBlockOnAnyDataNode(repoName, TimeValue.timeValueMinutes(1));

            logger.info("--> try deleting the snapshot while the restore is in progress (should throw an error)");
            ConcurrentSnapshotExecutionException e = expectThrows(
                ConcurrentSnapshotExecutionException.class,
                () -> clusterAdmin().prepareDeleteSnapshot(repoName, snapshotName).get()
            );
            assertEquals(repoName, e.getRepositoryName());
            assertEquals(snapshotName, e.getSnapshotName());
            assertThat(e.getMessage(), containsString("cannot delete snapshot during a restore"));
        } finally {
            // unblock even if the try block fails otherwise we will get bogus failures when we delete all indices in test teardown.
            logger.info("--> unblocking all data nodes");
            unblockAllDataNodes(repoName);
        }

        logger.info("--> wait for restore to finish");
        restoreFut.get();
    }

    private void waitForIndex(final String index, TimeValue timeout) throws Exception {
        assertBusy(
            () -> assertTrue("Expected index [" + index + "] to exist", indexExists(index)),
            timeout.millis(),
            TimeUnit.MILLISECONDS
        );
    }

    public void testSnapshotName() throws Exception {
        disableRepoConsistencyCheck("This test does not create any data in the repository");

        final Client client = client();

        createRepository("test-repo", "fs");

        expectThrows(InvalidSnapshotNameException.class, () -> client.admin().cluster().prepareCreateSnapshot("test-repo", "_foo").get());
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots("test-repo").setSnapshots("_foo").get()
        );
        expectThrows(SnapshotMissingException.class, () -> client.admin().cluster().prepareDeleteSnapshot("test-repo", "_foo").get());
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("_foo").get()
        );
    }

    public void testListCorruptedSnapshot() throws Exception {
        disableRepoConsistencyCheck("This test intentionally leaves a broken repository");

        Client client = client();
        Path repo = randomRepoPath();
        createRepository(
            "test-repo",
            "fs",
            Settings.builder().put("location", repo).put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        logger.info("--> indexing some data");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar"),
            client().prepareIndex("test-idx-3").setSource("foo", "bar")
        );

        logger.info("--> creating 2 snapshots");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-1")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices("test-idx-*")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> truncate snapshot file to make it unreadable");
        Path snapshotPath = repo.resolve("snap-" + createSnapshotResponse.getSnapshotInfo().snapshotId().getUUID() + ".dat");
        try (SeekableByteChannel outChan = Files.newByteChannel(snapshotPath, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }

        logger.info("--> get snapshots request should return both snapshots");
        List<SnapshotInfo> snapshotInfos = client.admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setIgnoreUnavailable(true)
            .get()
            .getSnapshots();

        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).snapshotId().getName(), equalTo("test-snap-1"));

        final SnapshotException ex = expectThrows(
            SnapshotException.class,
            () -> client.admin().cluster().prepareGetSnapshots("test-repo").setIgnoreUnavailable(false).get()
        );
        assertThat(ex.getRepositoryName(), equalTo("test-repo"));
        assertThat(ex.getSnapshotName(), equalTo("test-snap-2"));
    }

    /** Tests that a snapshot with a corrupted global state file can still be restored */
    public void testRestoreSnapshotWithCorruptedGlobalState() throws Exception {
        final Path repo = randomRepoPath();
        final String repoName = "test-repo";
        createRepository(repoName, "fs", repo);

        createIndex("test-idx-1", "test-idx-2");
        indexRandom(
            true,
            client().prepareIndex("test-idx-1").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar"),
            client().prepareIndex("test-idx-2").setSource("foo", "bar")
        );
        flushAndRefresh("test-idx-1", "test-idx-2");

        final String snapshotName = "test-snap";
        final SnapshotInfo snapshotInfo = createFullSnapshot(repoName, snapshotName);

        final Path globalStatePath = repo.resolve("meta-" + snapshotInfo.snapshotId().getUUID() + ".dat");
        try (SeekableByteChannel outChan = Files.newByteChannel(globalStatePath, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }

        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots(repoName).get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).snapshotId().getName(), equalTo(snapshotName));

        SnapshotsStatusResponse snapshotStatusResponse = clusterAdmin().prepareSnapshotStatus(repoName).setSnapshots(snapshotName).get();
        assertThat(snapshotStatusResponse.getSnapshots(), hasSize(1));
        assertThat(snapshotStatusResponse.getSnapshots().get(0).getSnapshot().getSnapshotId().getName(), equalTo(snapshotName));

        assertAcked(client().admin().indices().prepareDelete("test-idx-1", "test-idx-2"));

        SnapshotException ex = expectThrows(
            SnapshotException.class,
            () -> clusterAdmin().prepareRestoreSnapshot(repoName, snapshotName).setRestoreGlobalState(true).setWaitForCompletion(true).get()
        );
        assertThat(ex.getRepositoryName(), equalTo(repoName));
        assertThat(ex.getSnapshotName(), equalTo(snapshotName));
        assertThat(ex.getMessage(), containsString("failed to read global metadata"));

        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(snapshotInfo.successfulShards()));

        ensureGreen("test-idx-1", "test-idx-2");
        assertHitCount(client().prepareSearch("test-idx-*").setSize(0).get(), 3);
    }

    /**
     * Tests that a snapshot of multiple indices including one with a corrupted index metadata
     * file can still be used to restore the non corrupted indices
     * */
    public void testRestoreSnapshotWithCorruptedIndexMetadata() throws Exception {
        final Client client = client();
        final Path repo = randomRepoPath();
        final int nbIndices = randomIntBetween(2, 3);

        final Map<String, Integer> nbDocsPerIndex = new HashMap<>();
        for (int i = 0; i < nbIndices; i++) {
            String indexName = "test-idx-" + i;

            assertAcked(prepareCreate(indexName).setSettings(indexSettingsNoReplicas(Math.min(2, numberOfShards()))));

            int nbDocs = randomIntBetween(1, 10);
            nbDocsPerIndex.put(indexName, nbDocs);

            IndexRequestBuilder[] documents = new IndexRequestBuilder[nbDocs];
            for (int j = 0; j < nbDocs; j++) {
                documents[j] = client.prepareIndex(indexName).setSource("foo", "bar");
            }
            indexRandom(true, documents);
        }
        flushAndRefresh();

        createRepository("test-repo", "fs", repo);

        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");
        assertThat(snapshotInfo.indices(), hasSize(nbIndices));

        final RepositoryData repositoryData = getRepositoryData("test-repo");
        final Map<String, IndexId> indexIds = repositoryData.getIndices();
        assertThat(indexIds.size(), equalTo(nbIndices));

        // Choose a random index from the snapshot
        final IndexId corruptedIndex = randomFrom(indexIds.values());
        final Path indexMetadataPath = repo.resolve("indices")
            .resolve(corruptedIndex.getId())
            .resolve(
                "meta-" + repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotInfo.snapshotId(), corruptedIndex) + ".dat"
            );

        // Truncate the index metadata file
        try (SeekableByteChannel outChan = Files.newByteChannel(indexMetadataPath, StandardOpenOption.WRITE)) {
            outChan.truncate(randomInt(10));
        }

        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo").get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        assertThat(snapshotInfos.get(0).state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfos.get(0).snapshotId().getName(), equalTo("test-snap"));

        assertAcked(client().admin().indices().prepareDelete(nbDocsPerIndex.keySet().toArray(new String[nbDocsPerIndex.size()])));

        Predicate<String> isRestorableIndex = index -> corruptedIndex.getName().equals(index) == false;

        clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setIndices(nbDocsPerIndex.keySet().stream().filter(isRestorableIndex).toArray(String[]::new))
            .setRestoreGlobalState(randomBoolean())
            .setWaitForCompletion(true)
            .get();

        ensureGreen();
        for (Map.Entry<String, Integer> entry : nbDocsPerIndex.entrySet()) {
            if (isRestorableIndex.test(entry.getKey())) {
                assertHitCount(client().prepareSearch(entry.getKey()).setSize(0).get(), entry.getValue().longValue());
            }
        }

        assertAcked(startDeleteSnapshot("test-repo", snapshotInfo.snapshotId().getName()).get());
    }

    public void testCannotCreateSnapshotsWithSameName() throws Exception {
        final String repositoryName = "test-repo";
        final String snapshotName = "test-snap";
        final String indexName = "test-idx";
        final Client client = client();

        createRepository(repositoryName, "fs");
        logger.info("--> creating an index and indexing documents");
        createIndexWithRandomDocs(indexName, 10);

        logger.info("--> take first snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        logger.info("--> index more documents");
        for (int i = 10; i < 20; i++) {
            index(indexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> second snapshot of the same name should fail");
        try {
            createSnapshotResponse = client.admin()
                .cluster()
                .prepareCreateSnapshot(repositoryName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .get();
            fail(
                "should not be allowed to create a snapshot with the same name as an already existing snapshot: "
                    + createSnapshotResponse.getSnapshotInfo().snapshotId()
            );
        } catch (InvalidSnapshotNameException e) {
            assertThat(e.getMessage(), containsString("snapshot with the same name already exists"));
        }

        startDeleteSnapshot(repositoryName, snapshotName).get();

        logger.info("--> try creating a snapshot with the same name, now it should work because the first one was deleted");
        createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().snapshotId().getName(), equalTo(snapshotName));
    }

    /**
     * This test ensures that when a shard is removed from a node (perhaps due to the node
     * leaving the cluster, then returning), all snapshotting of that shard is aborted, so
     * all Store references held onto by the snapshot are released.
     * <p>
     * See https://github.com/elastic/elasticsearch/issues/20876
     */
    public void testSnapshotCanceledOnRemovedShard() throws Exception {
        final int numPrimaries = 1;
        final int numReplicas = 1;
        final String repo = "test-repo";
        final String index = "test-idx";
        final String snapshot = "test-snap";

        assertAcked(
            prepareCreate(index, 1, Settings.builder().put("number_of_shards", numPrimaries).put("number_of_replicas", numReplicas))
        );

        indexRandomDocs(index, 100);

        createRepository(
            repo,
            "mock",
            Settings.builder().put("location", randomRepoPath()).put("random", randomAlphaOfLength(10)).put("wait_after_unblock", 200)
        );

        String blockedNode = blockNodeWithIndex(repo, index);

        logger.info("--> snapshot");
        clusterAdmin().prepareCreateSnapshot(repo, snapshot).setWaitForCompletion(false).execute();

        logger.info("--> waiting for block to kick in on node [{}]", blockedNode);
        waitForBlock(blockedNode, repo, TimeValue.timeValueSeconds(10));

        logger.info("--> removing primary shard that is being snapshotted");
        ClusterState clusterState = internalCluster().clusterService(internalCluster().getClusterManagerName()).state();
        IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(index);
        String nodeWithPrimary = clusterState.nodes().get(indexRoutingTable.shard(0).primaryShard().currentNodeId()).getName();
        assertNotNull("should be at least one node with a primary shard", nodeWithPrimary);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeWithPrimary);
        IndexService indexService = indicesService.indexService(resolveIndex(index));
        indexService.removeShard(0, "simulate node removal");

        logger.info("--> unblocking blocked node [{}]", blockedNode);
        unblockNode(repo, blockedNode);

        logger.info("--> ensuring snapshot is aborted and the aborted shard was marked as failed");
        SnapshotInfo snapshotInfo = waitForCompletion(repo, snapshot, TimeValue.timeValueSeconds(60));
        assertEquals(1, snapshotInfo.shardFailures().size());
        assertEquals(0, snapshotInfo.shardFailures().get(0).shardId());
        assertThat(snapshotInfo.shardFailures().get(0).reason(), is("aborted"));
    }

    public void testSnapshotSucceedsAfterSnapshotFailure() throws Exception {
        // TODO: Fix repo cleanup logic to handle these leaked snap-file and only exclude test-repo (the mock repo) here.
        disableRepoConsistencyCheck(
            "This test uses a purposely broken repository implementation that results in leaking snap-{uuid}.dat files"
        );
        logger.info("--> creating repository");
        final Path repoPath = randomRepoPath();
        final Client client = client();
        Settings.Builder settings = Settings.builder()
            .put("location", repoPath)
            .put("random_control_io_exception_rate", randomIntBetween(5, 20) / 100f)
            // test that we can take a snapshot after a failed one, even if a partial index-N was written
            .put("random", randomAlphaOfLength(10));
        OpenSearchIntegTestCase.putRepository(client.admin().cluster(), "test-repo", "mock", false, settings);

        assertAcked(
            prepareCreate("test-idx").setSettings(
                // the less the number of shards, the less control files we have, so we are giving a higher probability of
                // triggering an IOException toward the end when writing the pending-index-* files, which are the files
                // that caused problems with writing subsequent snapshots if they happened to be lingering in the repository
                indexSettingsNoReplicas(1)
            )
        );
        ensureGreen();
        final int numDocs = randomIntBetween(1, 5);
        indexRandomDocs("test-idx", numDocs);

        logger.info("--> snapshot with potential I/O failures");
        try {
            CreateSnapshotResponse createSnapshotResponse = client.admin()
                .cluster()
                .prepareCreateSnapshot("test-repo", "test-snap")
                .setWaitForCompletion(true)
                .setIndices("test-idx")
                .get();
            if (createSnapshotResponse.getSnapshotInfo().totalShards() != createSnapshotResponse.getSnapshotInfo().successfulShards()) {
                assertThat(getFailureCount("test-repo"), greaterThan(0L));
                assertThat(createSnapshotResponse.getSnapshotInfo().shardFailures().size(), greaterThan(0));
                for (SnapshotShardFailure shardFailure : createSnapshotResponse.getSnapshotInfo().shardFailures()) {
                    assertThat(shardFailure.reason(), endsWith("; nested: IOException[Random IOException]"));
                }
            }
        } catch (SnapshotException | RepositoryException ex) {
            // sometimes, the snapshot will fail with a top level I/O exception
            assertThat(ExceptionsHelper.stackTrace(ex), containsString("Random IOException"));
        }

        logger.info("--> snapshot with no I/O failures");
        createRepository("test-repo-2", "mock", repoPath);
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo-2", "test-snap-2")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .get();
        assertEquals(0, createSnapshotResponse.getSnapshotInfo().failedShards());
        assertEquals(SnapshotState.SUCCESS, getSnapshot("test-repo-2", "test-snap-2").state());
    }

    public void testGetSnapshotsFromIndexBlobOnly() throws Exception {
        logger.info("--> creating repository");
        final Path repoPath = randomRepoPath();
        final Client client = client();
        Settings.Builder settings = Settings.builder().put("location", repoPath);
        OpenSearchIntegTestCase.putRepository(client.admin().cluster(), "test-repo", "fs", false, settings);

        logger.info("--> creating random number of indices");
        final int numIndices = randomIntBetween(1, 10);
        for (int i = 0; i < numIndices; i++) {
            assertAcked(prepareCreate("test-idx-" + i).setSettings(indexSettingsNoReplicas(1)));
        }

        logger.info("--> creating random number of snapshots");
        final int numSnapshots = randomIntBetween(1, 10);
        final Map<String, List<String>> indicesPerSnapshot = new HashMap<>();
        for (int i = 0; i < numSnapshots; i++) {
            // index some additional docs (maybe) for each index
            for (int j = 0; j < numIndices; j++) {
                if (randomBoolean()) {
                    final int numDocs = randomIntBetween(1, 5);
                    for (int k = 0; k < numDocs; k++) {
                        index("test-idx-" + j, "_doc", Integer.toString(k), "foo", "bar" + k);
                    }
                    refresh();
                }
            }
            final boolean all = randomBoolean();
            boolean atLeastOne = false;
            List<String> indices = new ArrayList<>();
            for (int j = 0; j < numIndices; j++) {
                if (all || randomBoolean() || !atLeastOne) {
                    indices.add("test-idx-" + j);
                    atLeastOne = true;
                }
            }
            final String snapshotName = "test-snap-" + i;
            indicesPerSnapshot.put(snapshotName, indices);
            createSnapshot("test-repo", snapshotName, indices);
        }

        logger.info("--> verify _all returns snapshot info");
        GetSnapshotsResponse response = clusterAdmin().prepareGetSnapshots("test-repo").setSnapshots("_all").setVerbose(false).get();
        assertEquals(indicesPerSnapshot.size(), response.getSnapshots().size());
        verifySnapshotInfo(response, indicesPerSnapshot);

        logger.info("--> verify wildcard returns snapshot info");
        response = clusterAdmin().prepareGetSnapshots("test-repo").setSnapshots("test-snap-*").setVerbose(false).get();
        assertEquals(indicesPerSnapshot.size(), response.getSnapshots().size());
        verifySnapshotInfo(response, indicesPerSnapshot);

        logger.info("--> verify individual requests return snapshot info");
        for (int i = 0; i < numSnapshots; i++) {
            response = clusterAdmin().prepareGetSnapshots("test-repo").setSnapshots("test-snap-" + i).setVerbose(false).get();
            assertEquals(1, response.getSnapshots().size());
            verifySnapshotInfo(response, indicesPerSnapshot);
        }
    }

    public void testSnapshottingWithMissingSequenceNumbers() {
        final String repositoryName = "test-repo";
        final String snapshotName = "test-snap";
        final String indexName = "test-idx";
        final Client client = client();

        createRepository(repositoryName, "fs");
        logger.info("--> creating an index and indexing documents");
        final String dataNode = internalCluster().getDataNodeInstance(ClusterService.class).localNode().getName();
        final Settings settings = indexSettingsNoReplicas(1).put("index.routing.allocation.include._name", dataNode).build();
        createIndex(indexName, settings);
        ensureGreen();
        for (int i = 0; i < 5; i++) {
            index(indexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }

        final Index index = resolveIndex(indexName);
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, dataNode).getShardOrNull(new ShardId(index, 0));
        // create a gap in the sequence numbers
        EngineTestCase.generateNewSeqNo(getEngineFromShard(primary));

        for (int i = 5; i < 10; i++) {
            index(indexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }

        refresh();

        createSnapshot(repositoryName, snapshotName, Collections.singletonList(indexName));

        logger.info("--> delete indices");
        assertAcked(client.admin().indices().prepareDelete(indexName));

        logger.info("--> restore all indices from the snapshot");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        IndicesStatsResponse stats = client().admin().indices().prepareStats(indexName).clear().get();
        ShardStats shardStats = stats.getShards()[0];
        assertTrue(shardStats.getShardRouting().primary());
        assertThat(shardStats.getSeqNoStats().getLocalCheckpoint(), equalTo(10L)); // 10 indexed docs and one "missing" op.
        assertThat(shardStats.getSeqNoStats().getGlobalCheckpoint(), equalTo(10L));
        logger.info("--> indexing some more");
        for (int i = 10; i < 15; i++) {
            index(indexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        client().admin().indices().prepareFlush(indexName).setForce(true).setWaitIfOngoing(true).get();

        stats = client().admin().indices().prepareStats(indexName).clear().get();
        shardStats = stats.getShards()[0];
        assertTrue(shardStats.getShardRouting().primary());
        assertThat(shardStats.getSeqNoStats().getLocalCheckpoint(), equalTo(15L)); // 15 indexed docs and one "missing" op.
        assertThat(shardStats.getSeqNoStats().getGlobalCheckpoint(), equalTo(15L));
        assertThat(shardStats.getSeqNoStats().getMaxSeqNo(), equalTo(15L));
    }

    public void testSnapshotDifferentIndicesBySameName() throws InterruptedException, ExecutionException {
        String indexName = "testindex";
        String repoName = "test-repo";
        Path absolutePath = randomRepoPath().toAbsolutePath();
        logger.info("Path [{}]", absolutePath);

        final int initialShardCount = randomIntBetween(1, 10);
        createIndex(indexName, Settings.builder().put("index.number_of_shards", initialShardCount).build());
        ensureGreen();

        final int docCount = initialShardCount * randomIntBetween(1, 10);
        indexRandomDocs(indexName, docCount);

        createRepository(repoName, "fs", absolutePath);

        logger.info("--> snapshot with [{}] shards", initialShardCount);
        final SnapshotInfo snapshot1 = createFullSnapshot(repoName, "snap-1");
        assertThat(snapshot1.successfulShards(), is(initialShardCount));

        logger.info("--> delete index");
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final int newShardCount = randomIntBetween(1, 10);
        createIndex(indexName, Settings.builder().put("index.number_of_shards", newShardCount).build());
        ensureGreen();

        final int newDocCount = newShardCount * randomIntBetween(1, 10);
        indexRandomDocs(indexName, newDocCount);

        logger.info("--> snapshot with [{}] shards", newShardCount);
        final SnapshotInfo snapshot2 = createFullSnapshot(repoName, "snap-2");
        assertThat(snapshot2.successfulShards(), is(newShardCount));

        logger.info("--> restoring snapshot 1");
        clusterAdmin().prepareRestoreSnapshot(repoName, "snap-1")
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement("restored-1")
            .setWaitForCompletion(true)
            .get();

        logger.info("--> restoring snapshot 2");
        clusterAdmin().prepareRestoreSnapshot(repoName, "snap-2")
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement("restored-2")
            .setWaitForCompletion(true)
            .get();

        logger.info("--> verify doc counts");
        assertDocCount("restored-1", docCount);
        assertDocCount("restored-2", newDocCount);

        final String snapshotToDelete;
        final String snapshotToRestore;
        final int expectedCount;
        if (randomBoolean()) {
            snapshotToDelete = "snap-1";
            snapshotToRestore = "snap-2";
            expectedCount = newDocCount;
        } else {
            snapshotToDelete = "snap-2";
            snapshotToRestore = "snap-1";
            expectedCount = docCount;
        }
        assertAcked(startDeleteSnapshot(repoName, snapshotToDelete).get());
        logger.info("--> restoring snapshot [{}]", snapshotToRestore);
        clusterAdmin().prepareRestoreSnapshot(repoName, snapshotToRestore)
            .setIndices(indexName)
            .setRenamePattern(indexName)
            .setRenameReplacement("restored-3")
            .setWaitForCompletion(true)
            .get();

        logger.info("--> verify doc counts");
        assertDocCount("restored-3", expectedCount);
    }

    public void testBulkDeleteWithOverlappingPatterns() {
        final int numberOfSnapshots = between(5, 15);
        createRepository("test-repo", "fs");

        final String[] indices = { "test-idx-1", "test-idx-2", "test-idx-3" };
        createIndex(indices);
        ensureGreen();

        logger.info("--> creating {} snapshots ", numberOfSnapshots);
        for (int i = 0; i < numberOfSnapshots; i++) {
            for (int j = 0; j < 10; j++) {
                index(randomFrom(indices), "_doc", Integer.toString(i * 10 + j), "foo", "bar" + i * 10 + j);
            }
            refresh();
            logger.info("--> snapshot {}", i);
            CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot("test-repo", "test-snap-" + i)
                .setWaitForCompletion(true)
                .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            assertThat(
                createSnapshotResponse.getSnapshotInfo().successfulShards(),
                equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
            );
        }

        logger.info("--> deleting all snapshots");
        clusterAdmin().prepareDeleteSnapshot("test-repo", "test-snap-*", "*").get();
        final GetSnapshotsResponse getSnapshotsResponse = clusterAdmin().prepareGetSnapshots("test-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots(), empty());
    }

    public void testHiddenIndicesIncludedInSnapshot() throws InterruptedException {
        Client client = client();
        final String normalIndex = "normal-index";
        final String hiddenIndex = "hidden-index";
        final String dottedHiddenIndex = ".index-hidden";
        final String repoName = "test-repo";

        createRepository(repoName, "fs");

        logger.info("--> creating indices");
        createIndex(normalIndex, indexSettingsNoReplicas(randomIntBetween(1, 3)).build());
        createIndex(hiddenIndex, indexSettingsNoReplicas(randomIntBetween(1, 3)).put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build());
        createIndex(
            dottedHiddenIndex,
            indexSettingsNoReplicas(randomIntBetween(1, 3)).put(IndexMetadata.SETTING_INDEX_HIDDEN, true).build()
        );
        ensureGreen();

        indexRandomDocs(normalIndex, 100);
        indexRandomDocs(hiddenIndex, 100);
        indexRandomDocs(dottedHiddenIndex, 100);

        logger.info("--> taking a snapshot");
        final String snapName = "test-snap";
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapName)
            .setWaitForCompletion(true)
            .setIndices(randomFrom("*", "_all"))
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        List<SnapshotInfo> snapshotInfos = client.admin()
            .cluster()
            .prepareGetSnapshots(repoName)
            .setSnapshots(randomFrom(snapName, "_all", "*", "*-snap", "test*"))
            .get()
            .getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(Version.CURRENT));

        logger.info("--> deleting indices");
        cluster().wipeIndices(normalIndex, hiddenIndex, dottedHiddenIndex);

        // Verify that hidden indices get restored with a wildcard restore
        {
            RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(repoName, snapName)
                .setWaitForCompletion(true)
                .setIndices("*")
                .execute()
                .actionGet();
            assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
            assertThat(
                restoreSnapshotResponse.getRestoreInfo().successfulShards(),
                equalTo(restoreSnapshotResponse.getRestoreInfo().totalShards())
            );
            assertThat(restoreSnapshotResponse.getRestoreInfo().indices(), containsInAnyOrder(normalIndex, hiddenIndex, dottedHiddenIndex));
            ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
            assertThat(clusterState.getMetadata().hasIndex(normalIndex), equalTo(true));
            assertThat(clusterState.getMetadata().hasIndex(hiddenIndex), equalTo(true));
            assertThat(clusterState.getMetadata().hasIndex(dottedHiddenIndex), equalTo(true));
            cluster().wipeIndices(normalIndex, hiddenIndex, dottedHiddenIndex);
        }

        // Verify that exclusions work on hidden indices
        {
            RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(repoName, snapName)
                .setWaitForCompletion(true)
                .setIndices("*", "-.*")
                .execute()
                .actionGet();
            assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
            assertThat(
                restoreSnapshotResponse.getRestoreInfo().successfulShards(),
                equalTo(restoreSnapshotResponse.getRestoreInfo().totalShards())
            );
            assertThat(restoreSnapshotResponse.getRestoreInfo().indices(), containsInAnyOrder(normalIndex, hiddenIndex));
            ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
            assertThat(clusterState.getMetadata().hasIndex(normalIndex), equalTo(true));
            assertThat(clusterState.getMetadata().hasIndex(hiddenIndex), equalTo(true));
            assertThat(clusterState.getMetadata().hasIndex(dottedHiddenIndex), equalTo(false));
            cluster().wipeIndices(normalIndex, hiddenIndex);
        }

        // Verify that hidden indices can be restored with a non-star pattern
        {
            RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(repoName, snapName)
                .setWaitForCompletion(true)
                .setIndices("hid*")
                .execute()
                .actionGet();
            assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
            assertThat(
                restoreSnapshotResponse.getRestoreInfo().successfulShards(),
                equalTo(restoreSnapshotResponse.getRestoreInfo().totalShards())
            );
            assertThat(restoreSnapshotResponse.getRestoreInfo().indices(), containsInAnyOrder(hiddenIndex));
            ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
            assertThat(clusterState.getMetadata().hasIndex(normalIndex), equalTo(false));
            assertThat(clusterState.getMetadata().hasIndex(hiddenIndex), equalTo(true));
            assertThat(clusterState.getMetadata().hasIndex(dottedHiddenIndex), equalTo(false));
            cluster().wipeIndices(hiddenIndex);
        }

        // Verify that hidden indices can be restored by fully specified name
        {
            RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(repoName, snapName)
                .setWaitForCompletion(true)
                .setIndices(dottedHiddenIndex)
                .get();
            assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
            assertThat(
                restoreSnapshotResponse.getRestoreInfo().successfulShards(),
                equalTo(restoreSnapshotResponse.getRestoreInfo().totalShards())
            );
            assertThat(restoreSnapshotResponse.getRestoreInfo().indices(), containsInAnyOrder(dottedHiddenIndex));
            ClusterState clusterState = client.admin().cluster().prepareState().get().getState();
            assertThat(clusterState.getMetadata().hasIndex(normalIndex), equalTo(false));
            assertThat(clusterState.getMetadata().hasIndex(hiddenIndex), equalTo(false));
            assertThat(clusterState.getMetadata().hasIndex(dottedHiddenIndex), equalTo(true));
        }
    }

    public void testIndexLatestFailuresIgnored() throws Exception {
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        final MockRepository repository = (MockRepository) internalCluster().getCurrentClusterManagerNodeInstance(RepositoriesService.class)
            .repository(repoName);
        repository.setFailOnIndexLatest(true);
        createFullSnapshot(repoName, "snapshot-1");
        repository.setFailOnIndexLatest(false);
        createFullSnapshot(repoName, "snapshot-2");
        final long repoGenInIndexLatest = BytesRefUtils.bytesToLong(
            new BytesRef(Files.readAllBytes(repoPath.resolve(BlobStoreRepository.INDEX_LATEST_BLOB)))
        );
        assertEquals(getRepositoryData(repoName).getGenId(), repoGenInIndexLatest);

        createRepository(
            repoName,
            "fs",
            Settings.builder().put("location", repoPath).put(BlobStoreRepository.SUPPORT_URL_REPO.getKey(), false)
        );
        createFullSnapshot(repoName, "snapshot-3");
        final long repoGenInIndexLatest2 = BytesRefUtils.bytesToLong(
            new BytesRef(Files.readAllBytes(repoPath.resolve(BlobStoreRepository.INDEX_LATEST_BLOB)))
        );
        assertEquals("index.latest should not have been written to", repoGenInIndexLatest, repoGenInIndexLatest2);

        createRepository(repoName, "fs", repoPath);
        createFullSnapshot(repoName, "snapshot-4");
        final long repoGenInIndexLatest3 = BytesRefUtils.bytesToLong(
            new BytesRef(Files.readAllBytes(repoPath.resolve(BlobStoreRepository.INDEX_LATEST_BLOB)))
        );
        assertEquals(getRepositoryData(repoName).getGenId(), repoGenInIndexLatest3);
    }

    private void verifySnapshotInfo(final GetSnapshotsResponse response, final Map<String, List<String>> indicesPerSnapshot) {
        for (SnapshotInfo snapshotInfo : response.getSnapshots()) {
            final List<String> expected = snapshotInfo.indices();
            assertEquals(expected, indicesPerSnapshot.get(snapshotInfo.snapshotId().getName()));
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        }
    }
}
