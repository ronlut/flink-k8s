package com.ronlut.flinkjobcluster.filesystemha;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

public class FsCheckpointIDCounter implements CheckpointIDCounter {
    private static final Logger LOG = LoggerFactory.getLogger(FsCheckpointIDCounter.class);
    private static final String TMP_SUFFIX = "_tmp";
    private static final long COUNTER_START = 1;

    private final FileSystem fileSystem;
    private final Path checkpointFilePath;
    private final Path checkpointFileTmpPath;

    FsCheckpointIDCounter(FileSystem fileSystem, Path checkpointFilePath) {
        this.fileSystem = fileSystem;
        this.checkpointFilePath = checkpointFilePath;
        this.checkpointFileTmpPath = checkpointFilePath.suffix(TMP_SUFFIX);
    }

    public void start() throws IOException {
        if (!fileSystem.exists(checkpointFilePath)) {
            LOG.info("checkpoint file doesn't exist, starting from {}", COUNTER_START);
            setCount(COUNTER_START);
        }
    }

    public void shutdown(JobStatus jobStatus) throws IOException {
        if(jobStatus.isGloballyTerminalState()) {
            fileSystem.delete(checkpointFilePath, false);
        }
    }

    public long getAndIncrement() throws IOException {
        long currentValue = get();

        setCount(currentValue+1);

        return currentValue;
    }

    @Override
    public long get() {
        try (DataInputStream inputStream = new DataInputStream(fileSystem.open(checkpointFilePath))) {
            return inputStream.readLong();
        } catch (IOException ierr) {
            throw new IllegalStateException("Failed to read current value from file", ierr);
        }
    }

    public void setCount(long value) throws IOException {
        try (DataOutputStream outputStream = new DataOutputStream(fileSystem.create(checkpointFileTmpPath, OVERWRITE))) {
            outputStream.writeLong(value);
        }

        // this should be atomic
        fileSystem.rename(checkpointFileTmpPath, checkpointFilePath);
    }
}
