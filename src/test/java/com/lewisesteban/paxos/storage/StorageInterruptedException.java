package com.lewisesteban.paxos.storage;

@SuppressWarnings("unused")
public class StorageInterruptedException extends StorageException {
    public StorageInterruptedException() {
    }

    public StorageInterruptedException(String message) {
        super(message);
    }

    public StorageInterruptedException(String message, Throwable cause) {
        super(message, cause);
    }

    public StorageInterruptedException(Throwable cause) {
        super(cause);
    }
}
