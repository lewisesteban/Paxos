package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.paxosnode.listener.UnneededInstanceGossipper;
import com.lewisesteban.paxos.storage.InterruptibleAccessorContainer;
import com.lewisesteban.paxos.storage.virtual.VirtualFileSystem;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import junit.framework.TestCase;

import java.io.File;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

@SuppressWarnings("SameParameterValue")
public class PaxosTestCase extends TestCase {

    protected static final Command cmd1 = new Command("ONE", "client", 1);
    protected static final Command cmd2 = new Command("TWO", "client", 2);
    protected static final Command cmd3 = new Command("THREE", "client", 3);
    protected static final Command cmd4 = new Command("FOUR", "client", 4);
    protected static final Command cmd5 = new Command("FIVE", "client", 5);

    protected Random random = new Random();

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

    protected void cleanup() {
        File rootDir = new File(".");
        File[] files = rootDir.listFiles();
        if(files!=null) {
            for(File f: files) {
                if (f.getName().startsWith("proposer") || f.getName().startsWith("acceptor") ||
                        f.getName().endsWith("commandManager") || f.getName().startsWith("stateMachine") ||
                        f.getName().equals("test")) {
                    if(f.isDirectory()) {
                        deleteFolder(f);
                    } else {
                        //noinspection ResultOfMethodCallIgnored
                        f.delete();
                    }
                }
            }
        }
        InterruptibleAccessorContainer.clear();
        VirtualFileSystem.clear();
    }


    @Override
    public void tearDown() {
        cleanup();
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        VirtualFileSystem.clear();
        SnapshotManager.SNAPSHOT_FREQUENCY = 1000;
        UnneededInstanceGossipper.GOSSIP_FREQUENCY = 100;
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    protected static boolean isCause(Class<? extends Throwable> expected, Throwable exc) {
        return expected.isInstance(exc) || (exc != null && isCause(expected, exc.getCause()));
    }

    static public class TestCommand implements java.io.Serializable {

        public TestCommand(int clientId, int cmdNb) {
            this.clientId = clientId;
            this.cmdNb = cmdNb;
        }

        public int clientId;
        public int cmdNb;

        @Override
        public String toString() {
            return "client" + clientId + "cmd" + cmdNb;
        }
    }

    protected Thread serialKiller(Network network, List<PaxosNetworkNode> nodes, int duration, int maxSleep) {
        return serialKiller(network, nodes, duration, maxSleep, null);
    }

    protected Thread serialKiller(Network network, List<PaxosNetworkNode> nodes, int duration, int maxSleep, KillController killController) {
        final int nbNodes = nodes.size();

        Callable<Integer> nbNodesAlive = () -> {
            int alive = 0;
            for (PaxosNetworkNode node : nodes) {
                if (node.isRunning())
                    alive++;
            }
            return alive;
        };

        Runnable killLiveNode = () -> {
            int iterations = 0;
            int node = random.nextInt(nbNodes);
            while (!nodes.get(node).isRunning() && iterations < nbNodes) {
                node++;
                if (node == nbNodes)
                    node = 0;
                iterations++;
            }
            if (nodes.get(node).isRunning()) {
                System.out.println("--- killing " + node);
                if (killController != null)
                    killController.kill(node);
                network.kill(addr(node));
            }
        };

        Runnable restoreDeadNode = () -> {
            int iterations = 0;
            int node = random.nextInt(nbNodes);
            while (nodes.get(node).isRunning() && iterations < nbNodes) {
                node++;
                if (node == nbNodes)
                    node = 0;
                iterations++;
            }
            if (!nodes.get(node).isRunning()) {
                System.out.println("+++ restoring " + node);
                network.start(addr(node));
            }
        };

        return new Thread(() -> {
            long startTime = System.currentTimeMillis();
            boolean killingSpree = true;
            try {
                while (System.currentTimeMillis() - startTime < duration) {
                    try {
                        Thread.sleep(random.nextInt(maxSleep));
                    } catch (InterruptedException ignored) {
                    }
                    if (killingSpree) {
                        if (random.nextInt(4) == 0 && nbNodesAlive.call() < nbNodes)
                            restoreDeadNode.run();
                        else
                            killLiveNode.run();
                        if (nbNodesAlive.call() == 0)
                            killingSpree = false;
                    } else {
                        if (random.nextInt(4) == 0 && nbNodesAlive.call() > 0)
                            killLiveNode.run();
                        else
                            restoreDeadNode.run();
                        if (nbNodesAlive.call() == nbNodes)
                            killingSpree = true;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    protected Network.Address addr(int nodeId) {
        return new Network.Address(0, nodeId);
    }

    protected interface KillController {
        void kill(int nodeId);
    }
}
