package com.lewisesteban.paxos.storage;

import java.io.InputStream;
import java.io.OutputStream;

public interface FileAccessor {

    OutputStream startWrite() throws StorageException;
    void endWrite() throws StorageException;
    InputStream startRead() throws StorageException;
    void endRead() throws StorageException;
    void delete() throws StorageException;
    boolean exists();
    long length() throws StorageException;
    String getFilePath();
}
