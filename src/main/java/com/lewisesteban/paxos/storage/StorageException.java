package com.lewisesteban.paxos.storage;

import java.io.IOException;

@SuppressWarnings("unused")
public class StorageException extends IOException {
    public StorageException() {
        super();
    }

    public StorageException(String message) {
        super(message);
    }

    public StorageException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageException(Throwable cause) {
        super(cause);
    }
}
