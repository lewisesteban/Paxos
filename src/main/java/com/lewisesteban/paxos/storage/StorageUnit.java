package com.lewisesteban.paxos.storage;

import java.util.Iterator;
import java.util.Map;

public interface StorageUnit {

    Iterator<Map.Entry<String, String>> startReadAll() throws StorageException;
    String read(String key) throws StorageException;
    void put(String key, String value) throws StorageException;
    void flush() throws StorageException;
    void delete() throws StorageException;
    void close() throws StorageException;

    public interface Creator {
        StorageUnit make(String file, String dir) throws StorageException;
    }
}
