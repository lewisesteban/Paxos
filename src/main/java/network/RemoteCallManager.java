package network;

import java.io.IOException;

public interface RemoteCallManager {
    <T> T doRemoteCall(RemoteCallable<T> callable) throws IOException;

    interface RemoteCallable<T> {
        T doRemoteCall() throws IOException;
    }
}
