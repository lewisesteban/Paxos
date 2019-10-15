package com.lewisesteban.paxos.storage;

import java.io.IOException;

public interface FileAccessorCreator {
    FileAccessor create(String filePath, String directory) throws IOException;
}
