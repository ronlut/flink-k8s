package com.ronlut.flinkjobcluster.filesystemha;

import java.io.IOException;
import java.util.concurrent.Executor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.FileSystemBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.dispatcher.SingleJobSubmittedJobGraphStore;
import org.apache.flink.runtime.highavailability.FsNegativeRunningJobsRegistry;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link HighAvailabilityServices} implementation
 */
public class SingleFsHaServices implements HighAvailabilityServices {
    private static final String BLOB_STORE = "blobstore";
    private static final String RUNNING_JOBS_REGISTRY = "runningJobsRegistry";
    private static final String CHECKPOINT_RECOVERY = "checkpointRecovery";

    private final String resourceManagerAddress;
    private final String dispatcherAddress;
    private final String jobManagerAddress;
    private final String webMonitorAddress;
    private final Path root;
    private final Executor executor;

    SingleFsHaServices(String resourceManagerAddress,
                       String dispatcherAddress,
                       String jobManagerAddress,
                       String webMonitorAddress,
                       Path root,
                       Executor executor) {
        this.resourceManagerAddress = checkNotNull(resourceManagerAddress, "resourceManagerAddress");
        this.dispatcherAddress = checkNotNull(dispatcherAddress, "dispatcherAddress");
        this.jobManagerAddress = checkNotNull(jobManagerAddress, "jobManagerAddress");
        this.webMonitorAddress = checkNotNull(webMonitorAddress, webMonitorAddress);
        this.executor = checkNotNull(executor);
        this.root = checkNotNull(root);
    }

    public LeaderRetrievalService getResourceManagerLeaderRetriever() {
        return new StandaloneLeaderRetrievalService(resourceManagerAddress, DEFAULT_LEADER_ID);
    }

    public LeaderRetrievalService getDispatcherLeaderRetriever() {
        return new StandaloneLeaderRetrievalService(dispatcherAddress, DEFAULT_LEADER_ID);
    }

    public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
        return new StandaloneLeaderRetrievalService(jobManagerAddress, DEFAULT_LEADER_ID);
    }

    public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
        return new StandaloneLeaderRetrievalService(defaultJobManagerAddress, DEFAULT_LEADER_ID);
    }

    public LeaderRetrievalService getWebMonitorLeaderRetriever() {
        return new StandaloneLeaderRetrievalService(webMonitorAddress, DEFAULT_LEADER_ID);
    }

    public LeaderElectionService getResourceManagerLeaderElectionService() {
        return new StandaloneLeaderElectionService();
    }

    public LeaderElectionService getDispatcherLeaderElectionService() {
        return new StandaloneLeaderElectionService();
    }

    public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
        return new StandaloneLeaderElectionService();
    }

    public LeaderElectionService getWebMonitorLeaderElectionService() {
        return new StandaloneLeaderElectionService();
    }

    public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
        return new FsCheckpointRecoveryFactory(createFilesystem(), new Path(root, CHECKPOINT_RECOVERY), executor);
    }

    public SubmittedJobGraphStore getSubmittedJobGraphStore() {
        return new SingleJobSubmittedJobGraphStore(new JobGraph(DEFAULT_JOB_ID, null));
    }

    public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
        return new FsNegativeRunningJobsRegistry(createFilesystem(), new Path(root, RUNNING_JOBS_REGISTRY));
    }

    public BlobStore createBlobStore() throws IOException {
        return new FileSystemBlobStore(createFilesystem(), new Path(root, BLOB_STORE).getPath());
    }

    public void close() {
    }

    public void closeAndCleanupAllData() throws Exception {
        createFilesystem().delete(root, true);
    }

    private FileSystem createFilesystem() {
        try {
            return root.getFileSystem();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
