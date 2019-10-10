package com.lewisesteban.paxos.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public interface StorageUnit extends Closeable {

    Iterator<Map.Entry<String, String>> startReadAll() throws IOException;
    String read(String key) throws IOException;
    void write(String key, String value) throws IOException;
    void delete() throws IOException;
}
