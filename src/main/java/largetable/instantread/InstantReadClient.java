package largetable.instantread;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;

public class InstantReadClient {

    public static void main(String[] args) throws RemoteException, NotBoundException {
        if (args.length < 2) {
            System.out.println("Expected arguments: fragmentNb, nodeId, (optional) target IP address");
            return;
        }
        int fragment = Integer.parseInt(args[0]);
        int nodeId = Integer.parseInt(args[1]);
        String host = "127.0.0.1";
        if (args.length > 2)
            host = args[2];

        Registry registry = LocateRegistry.getRegistry(host);
        RemoteLTReader remoteLTReader = (RemoteLTReader) registry.lookup("instantReader_" + fragment + "_" + nodeId);
        Map<String, String> data = remoteLTReader.read();
        for (Map.Entry<String, String> entry : data.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}
