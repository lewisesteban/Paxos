package com.lewisesteban.paxos.storage;

import java.io.*;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class SafeSingleFileStorage implements StorageUnit {

    private FileManager fileManager;
    private TreeMap<String, String> content = null;

    public SafeSingleFileStorage(String fileName, String dir, FileAccessorCreator fileAccessorCreator) throws StorageException {
        try {
            fileManager = new FileManager(fileName, dir, fileAccessorCreator);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public StorageUnit overwriteMode() {
        content = new TreeMap<>();
        return this;
    }

    @Override
    public synchronized Iterator<Map.Entry<String, String>> startReadAll() throws StorageException {
        if (content == null) {
            try {
                readAllContent();
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
        return content.entrySet().iterator();
    }

    @Override
    public synchronized String read(String key) throws StorageException {
        if (content == null) {
            try {
                readAllContent();
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
        return content.getOrDefault(key, null);
    }

    @Override
    public synchronized void put(String key, String value) throws StorageException {
        if (content == null) {
            try {
                readAllContent();
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
        content.put(key, value);
    }

    @Override
    public void flush() throws StorageException {
        try {
            OutputStream writer = fileManager.startWrite();
            ObjectOutputStream oos = new ObjectOutputStream(writer);
            oos.writeObject(content);
            oos.flush();
            fileManager.endWrite();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public synchronized void delete() throws StorageException {
        try {
            fileManager.deleteAll();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    private void readAllContent() throws StorageException {
        InputStream reader = fileManager.startRead();
        if (reader == null) {
            content = new TreeMap<>();
            return;
        }
        try {
            try {
                ObjectInputStream ois = new ObjectInputStream(reader);
                Object res = ois.readObject();
                //noinspection unchecked
                content = (TreeMap<String, String>) res;
                fileManager.endRead();
            } catch (StorageException e) {
                throw e;
            } catch (IOException e) {
                reader = fileManager.fixCorruption();
                if (reader == null) {
                    content = new TreeMap<>();
                    return;
                }
                ObjectInputStream ois = new ObjectInputStream(reader);
                Object res = ois.readObject();
                //noinspection unchecked
                content = (TreeMap<String, String>) res;
                fileManager.endRead();
            }

        } catch (IOException | ClassNotFoundException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public synchronized boolean isEmpty() throws StorageException {
        if (content == null) {
            try {
                readAllContent();
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
        return content.isEmpty();
    }

    @Override
    public synchronized void close() throws StorageException {
        try {
            fileManager.close();
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    /**
     * Writes into two files in a round-robin fashion, in case one file gets corrupted
     */
    class FileManager {
        private FileAccessor mainFile;
        private FileAccessor tmpFile;

        FileManager(String fileName, String dir, FileAccessorCreator fileAccessorCreator) throws StorageException {
            try {
                mainFile = fileAccessorCreator.create(fileName, dir);
                tmpFile = fileAccessorCreator.create(fileName + "_tmp", dir);
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }

        OutputStream startWrite() throws StorageException {
            return tmpFile.startWrite();
        }

        void endWrite() throws StorageException {
            tmpFile.endWrite();
            moveTempToMain();
        }

        InputStream startRead() throws StorageException {
            if (!mainFile.exists()) {
                if (tmpFile.exists()) {
                    moveTempToMain();
                } else {
                    return null;
                }
            }
            return mainFile.startRead();
        }

        void endRead() throws StorageException {
            mainFile.endRead();
        }

        InputStream fixCorruption() throws StorageException {
            endRead();
            if (tmpFile.exists()) {
                moveTempToMain();
                return mainFile.startRead();
            } else {
                return null;
            }
        }

        private void moveTempToMain() throws StorageException {
            if (tmpFile.length() == 0)
                return;
            if (mainFile.exists()) {
                mainFile.delete();
            }
            try {
                tmpFile.moveTo(mainFile, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }

        void deleteAll() throws StorageException {
            mainFile.delete();
            try {
                tmpFile.delete();
            } catch (IOException ignored) { }

        }

        void close() throws StorageException {
            mainFile.endRead();
            mainFile.endWrite();
            tmpFile.endRead();
            tmpFile.endWrite();
        }
    }
}