package largetable.instantread;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

public interface RemoteLTReader extends Remote {
    Map<String, String> read() throws RemoteException;
}
