package largetable.instantread;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;

public class InstantReadServer implements RemoteLTReader {
    private LTReader largeTable;

    public InstantReadServer(LTReader largeTable, String name) {
        this.largeTable = largeTable;
        new Thread(() -> {
            try {
                RemoteLTReader stub = (RemoteLTReader) UnicastRemoteObject.exportObject(this, 0);
                Registry registry = LocateRegistry.getRegistry();
                registry.rebind("instantReader_" + name, stub);
            } catch (RemoteException ignored) {
            }
        }).start();
    }


    @Override
    public Map<String, String> read() {
        return largeTable.read();
    }
}
