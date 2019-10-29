package com.lewisesteban.paxos;

import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.storage.InterruptibleAccessorContainer;
import com.lewisesteban.paxos.storage.virtual.VirtualFileSystem;
import junit.framework.TestCase;

import java.io.File;

public class PaxosTestCase extends TestCase {

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
                if (f.getName().startsWith("proposer") || f.getName().startsWith("acceptor") || f.getName().equals("test")) {
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
        SnapshotManager.SNAPSHOT_FREQUENCY = 100;
        SnapshotManager.KEEP_AFTER_SNAPSHOT = 30;
    }

    protected static boolean isCause(Class<? extends Throwable> expected, Throwable exc) {
        return expected.isInstance(exc) || (exc != null && isCause(expected, exc.getCause()));
    }
}
