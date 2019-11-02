package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.storage.InterruptibleAccessorContainer;
import com.lewisesteban.paxos.storage.virtual.VirtualFileSystem;
import junit.framework.TestCase;

import java.io.File;
import java.util.Random;

public class PaxosTestCase extends TestCase {

    protected static final Command cmd1 = new Command("ONE", "client", 1);
    protected static final Command cmd2 = new Command("TWO", "client", 2);
    protected static final Command cmd3 = new Command("THREE", "client", 3);
    protected static final Command cmd4 = new Command("FOUR", "client", 4);
    protected static final Command cmd5 = new Command("FIVE", "client", 5);
    protected static final Command cmd6 = new Command("SIX", "client", 6);

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
        VirtualFileSystem.clear();
        SnapshotManager.SNAPSHOT_FREQUENCY = 1000;
    }

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
}
