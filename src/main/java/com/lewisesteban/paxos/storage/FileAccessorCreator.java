package com.lewisesteban.paxos.storage;

public interface FileAccessorCreator {
    FileAccessor create(String filePath, String directory) throws StorageException;
}
