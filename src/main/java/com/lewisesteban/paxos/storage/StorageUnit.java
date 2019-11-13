package com.lewisesteban.paxos.storage;

import java.util.Iterator;
import java.util.Map;

public interface StorageUnit {

    /**
     * Call this method just after creating the StorageUnit instance in order to ignore and overwriteMode the already-
     * existing file if there is one.
     *
     * @return The storage unit
     */
    StorageUnit overwriteMode();

    Iterator<Map.Entry<String, String>> startReadAll() throws StorageException;

    String read(String key) throws StorageException;

    /**
     * Updates an entry, or creates it if it doesn't exist.
     * The entry will only be written in memory. Call flush() to write it on the disk.
     */
    void put(String key, String value) throws StorageException;

    void flush() throws StorageException;

    void delete() throws StorageException;

    void close() throws StorageException;

    boolean isEmpty() throws StorageException;

    interface Creator {
        StorageUnit make(String file, String dir) throws StorageException;
    }
}
