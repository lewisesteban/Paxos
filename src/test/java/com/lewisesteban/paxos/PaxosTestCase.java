package com.lewisesteban.paxos;

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

    private void cleanup() {
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
    }


    @Override
    public void tearDown() {
        cleanup();
    }
}
