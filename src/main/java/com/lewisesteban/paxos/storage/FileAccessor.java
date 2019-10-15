package com.lewisesteban.paxos.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface FileAccessor {

    OutputStream startWrite() throws IOException;
    void endWrite() throws IOException;
    InputStream startRead() throws IOException;
    void endRead() throws IOException;
    void delete() throws IOException;
    boolean exists();
    long length() throws IOException;
    String getFilePath();
}
