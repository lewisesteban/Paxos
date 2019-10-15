package com.lewisesteban.paxos.storage;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;

public interface StorageUnit extends Closeable {

    Iterator<Map.Entry<String, String>> startReadAll() throws StorageException;
    String read(String key) throws StorageException;
    void put(String key, String value) throws StorageException;
    void flush() throws StorageException;
    void delete() throws StorageException;
}
