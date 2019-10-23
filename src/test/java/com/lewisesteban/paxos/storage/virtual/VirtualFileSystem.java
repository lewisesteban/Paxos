package com.lewisesteban.paxos.storage.virtual;

import com.lewisesteban.paxos.storage.StorageException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@SuppressWarnings("SynchronizeOnNonFinalField")
public class VirtualFileSystem {
    private static Map<String, VirtualFile> files = new TreeMap<>();

    static VirtualFile create(String path) {
        synchronized (files) {
            VirtualFile newFile = new VirtualFile();
            files.put(path, newFile);
            return newFile;
        }
    }

    static void delete(String path) {
        synchronized (files) {
            files.remove(path);
        }
    }

    static boolean exists(String path) {
        synchronized (files) {
            return files.containsKey(path);
        }
    }

    static VirtualFile get(String path) {
        synchronized (files) {
            return files.getOrDefault(path, null);
        }
    }

    static void move(String src, String dest) throws StorageException {
        synchronized (files) {
            if (!files.containsKey(src))
                throw new StorageException("File not found: " + src);
            files.put(dest, files.get(src));
            files.remove(src);
        }
    }

    public static void clear() {
        files = new TreeMap<>();
    }

    static List<String> listFiles(String dir) {
        List<String> list = new ArrayList<>();
        synchronized (files) {
            for (String path : files.keySet()) {
                if (path.startsWith(dir + File.separator)) {
                    list.add(path);
                }
            }
        }
        if (list.size() == 0)
            return null;
        return list;
    }
}
