package com.ronlut.flinkjobcluster.filesystemha;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.ThrowingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.core.fs.FileSystem.WriteMode.NO_OVERWRITE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

class FsCompletedCheckpointStore implements CompletedCheckpointStore {
    private static final Logger LOG = LoggerFactory.getLogger(FsCompletedCheckpointStore.class);

    private final FileSystem fileSystem;
    private final Path checkpointStore;
    private final int maxNumberOfRetainedCheckpoints;
    private final Executor executor;

    private ArrayDeque<CompletedCheckpoint> completedCheckpoints;

    FsCompletedCheckpointStore(FileSystem fileSystem, Path checkpointStore, int maxNumberOfRetainedCheckpoints, Executor executor) {
        checkArgument(maxNumberOfRetainedCheckpoints >= 1, "Must retain at least one checkpoint.");

        this.fileSystem = checkNotNull(fileSystem);
        this.checkpointStore =  checkNotNull(checkpointStore);
        this.maxNumberOfRetainedCheckpoints = maxNumberOfRetainedCheckpoints;
        this.completedCheckpoints = new ArrayDeque<>(maxNumberOfRetainedCheckpoints + 1);
        this.executor = checkNotNull(executor);
    }

    @Override
    public void recover() throws Exception {
        completedCheckpoints = new ArrayDeque<>();

        if (!fileSystem.exists(checkpointStore)) {
            fileSystem.mkdirs(checkpointStore);
            return;
        }

        //retrieve checkpoints from files in checkpointStore directory
        for (FileStatus fileStatus : fileSystem.listStatus(checkpointStore)) {
            if (fileStatus.isDir()) {
                throw new RuntimeException("Found unexpected directory in checkpoint store: " + checkpointStore.getPath());
            }
            completedCheckpoints.addLast(completedCheckpointFromFile(fileStatus.getPath()));
        }
    }

    private CompletedCheckpoint completedCheckpointFromFile(Path path) throws Exception {
        try (FSDataInputStream inputStream = fileSystem.open(path)) {
            return InstantiationUtil.deserializeObject(inputStream, Thread.currentThread().getContextClassLoader());
        }
    }

    @Override
    public void addCheckpoint(CompletedCheckpoint completedCheckpoint) throws Exception {
        try (FSDataOutputStream outputStream = fileSystem.create(getPath(completedCheckpoint), NO_OVERWRITE)) {
            InstantiationUtil.serializeObject(outputStream, completedCheckpoint);
            completedCheckpoints.addLast(completedCheckpoint);
        }

        // Everything worked, let's remove previous checkpoints if necessary.
        while (completedCheckpoints.size() > maxNumberOfRetainedCheckpoints) {
            final CompletedCheckpoint oldCheckpoint = completedCheckpoints.removeFirst();
            tryRemoveCompletedCheckpoint(oldCheckpoint, CompletedCheckpoint::discardOnSubsume);
        }
    }

    @Override
    public void shutdown(JobStatus jobStatus) throws Exception {
        if (jobStatus.isGloballyTerminalState()) {
            LOG.info("Shutting down");

            for (CompletedCheckpoint checkpoint : completedCheckpoints) {
                tryRemoveCompletedCheckpoint(
                        checkpoint,
                        completedCheckpoint -> completedCheckpoint.discardOnShutdown(jobStatus));
            }

            fileSystem.delete(checkpointStore, true);

        } else {
            LOG.info("Suspending");
        }

        completedCheckpoints.clear();
    }

    private void tryRemoveCompletedCheckpoint(CompletedCheckpoint completedCheckpoint, ThrowingConsumer<CompletedCheckpoint, Exception> discardCallback) {
        try {
            if (tryRemove(getPath(completedCheckpoint))) {
                executor.execute(() -> {
                    try {
                        discardCallback.accept(completedCheckpoint);
                    } catch (Exception e) {
                        LOG.warn("Could not discard completed checkpoint {}.", completedCheckpoint.getCheckpointID(), e);
                    }
                });

            }
        } catch (Exception e) {
            LOG.warn("Failed to subsume the old checkpoint", e);
        }
    }

    private boolean tryRemove(Path checkpointPath) throws Exception {
        return fileSystem.delete(checkpointPath, false);
    }

    @Override
    public List<CompletedCheckpoint> getAllCheckpoints() {
        return new ArrayList<>(completedCheckpoints);
    }

    @Override
    public int getNumberOfRetainedCheckpoints() {
        return completedCheckpoints.size();
    }

    @Override
    public int getMaxNumberOfRetainedCheckpoints() {
        return maxNumberOfRetainedCheckpoints;
    }

    @Override
    public boolean requiresExternalizedCheckpoints() {
        return true;
    }

    private Path getPath(CompletedCheckpoint completedCheckpoint) {
        return new Path(checkpointStore, String.valueOf(completedCheckpoint.getCheckpointID()));
    }
}
