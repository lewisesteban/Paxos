package network;

import com.lewisesteban.paxos.paxosnode.PaxosNode;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.WholeFileAccessor;
import junit.framework.TestCase;
import largetable.Client;
import largetable.LargeTableClient;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DistributedTest extends TestCase {
    // TODO what if multiple prepare attempts are needed because propId is too low?
    // --> when server starts, ask highest-numbered server alive for last inst and propNumber (async)
    private final int nbServers = 5;
    private int nbServersStarted = 0;

    public void test1SimpleCommands() throws RemoteException, StorageException, Client.LargeTableException {
        for (int srvNb = nbServersStarted + 1; srvNb < nbServers - 1; srvNb++) {
            startServer(srvNb);
            nbServersStarted++;
        }

        LargeTableClient client1 = startClient("1");
        LargeTableClient client2 = startClient("2");
        client1.put("key", "val");
        assertEquals("val", client1.get("key"));
        client2.append("key", "+");
        assertEquals("val+", client1.get("key"));
        LargeTableClient client3 = startClient("3");
        assertEquals("val+", client3.get("key"));
        client3.append("key2", "a");
        assertEquals("a", client2.get("key2"));
        assertEquals("a", client2.recover().getResult());
        assertEquals("key2", client3.recover().getCmdKey());
        assertEquals("a", client3.recover().getCmdValue());
        assertEquals(Client.ExecutedCommand.TYPE_APPEND, client3.recover().getCmdType());
    }

    public void test2CatchingUp() throws RemoteException, StorageException, Client.LargeTableException, InterruptedException {
        SnapshotManager.SNAPSHOT_FREQUENCY = 1000;
        int nbCmds = 100;

        System.out.println("starting");
        for (int srvNb = nbServersStarted + 1; srvNb < nbServers - 1; srvNb++) {
            startServer(srvNb);
            nbServersStarted++;
        }
        LargeTableClient client4 = startClient("4");

        System.out.println();
        System.out.println("doing commands");
        for (int i = 0; i < nbCmds; ++i) {
            client4.append("str100", "a");
        }
        assertEquals(nbCmds, client4.get("str100").length());
        assertEquals(nbCmds + 1, Objects.requireNonNull(new File("acceptor3").listFiles()).length);

        System.out.println();
        System.out.println("testing backward catching-up");
        startServer(0);
        Thread.sleep(100); // wait for server to connect to others, so that CUM is initialized
        client4.get("str100");
        Thread.sleep(3000); // wait for server to catch-up
        assertEquals(nbCmds + 2, Objects.requireNonNull(new File("acceptor0").listFiles()).length);

        System.out.println();
        System.out.println("testing forward catching-up");
        startServer(4);
        Thread.sleep(1000); // wait for election
        LargeTableClient client5 = startClient("5");
        assertEquals(nbCmds, client5.get("str100").length());
        assertEquals(nbCmds + 3, Objects.requireNonNull(new File("acceptor4").listFiles()).length);
    }

    private void startServer(int serverId) throws StorageException, RemoteException {
        List<RemotePaxosNode> cluster = new ArrayList<>();
        PaxosNode paxosNode = new PaxosNode(serverId, 0, cluster,
                new largetable.Server(WholeFileAccessor::new),
                (f, dir) -> new SafeSingleFileStorage(f, dir, WholeFileAccessor::new),
                WholeFileAccessor::new);
        NodeServer server = new NodeServer(paxosNode);
        List<MultiClientCatchingUpManager.ClientCUMGetter> catchingUpManagers = new ArrayList<>();
        for (int remoteSrvId = 0; remoteSrvId < nbServers; remoteSrvId++) {
            if (remoteSrvId == server.getId()) {
                cluster.add(server);
            } else {
                NodeClient nodeClient = new NodeClient("127.0.0.1", remoteSrvId, 0);
                cluster.add(nodeClient);
                catchingUpManagers.add(nodeClient.getCatchingUpManager());
            }
        }
        paxosNode.setCatchingUpManager(new MultiClientCatchingUpManager(catchingUpManagers));
        server.start();
    }

    private LargeTableClient startClient(String clientId) throws Client.LargeTableException, StorageException {
        List<NodeClient> cluster = new ArrayList<>();
        for (int srvId = 0; srvId < nbServers; srvId++) {
            cluster.add(new NodeClient("127.0.0.1", srvId, 0));
        }
        LargeTableClient client = new LargeTableClient<>(cluster, clientId, WholeFileAccessor::new);
        client.recover();
        return client;
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        clear();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        clear();
    }

    private static void clear() {
        File rootDir = new File(".");
        File[] files = rootDir.listFiles();
        if(files!=null) {
            for(File f: files) {
                if (f.getName().startsWith("proposer") || f.getName().startsWith("acceptor") ||
                        f.getName().contains("commandManager") || f.getName().startsWith("stateMachine") ||
                        f.getName().contains("clientOngoingCommand") || f.getName().equals("test")) {
                    if(f.isDirectory()) {
                        deleteFolder(f);
                    } else {
                        //noinspection ResultOfMethodCallIgnored
                        f.delete();
                    }
                }
            }
        }
    }

    private static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) {
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    //noinspection ResultOfMethodCallIgnored
                    f.delete();
                }
            }
        }
        //noinspection ResultOfMethodCallIgnored
        folder.delete();
    }
}
