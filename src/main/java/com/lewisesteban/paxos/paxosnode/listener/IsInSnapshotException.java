package com.lewisesteban.paxos.paxosnode.listener;

@SuppressWarnings("unused")
public class IsInSnapshotException extends Exception {
    IsInSnapshotException() {
    }

    IsInSnapshotException(String message) {
        super(message);
    }

    IsInSnapshotException(String message, Throwable cause) {
        super(message, cause);
    }

    IsInSnapshotException(Throwable cause) {
        super(cause);
    }
}
