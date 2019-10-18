package com.lewisesteban.paxos.storage;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.CopyOption;

public interface FileAccessor {

    OutputStream startWrite() throws StorageException;
    void endWrite() throws StorageException;
    InputStream startRead() throws StorageException;
    void endRead() throws StorageException;
    void delete() throws StorageException;
    boolean exists();
    long length() throws StorageException;
    String getFilePath();
    String getName();
    void moveTo(FileAccessor dest, CopyOption copyOption) throws StorageException;
    FileAccessor[] listFiles() throws StorageException;
}
