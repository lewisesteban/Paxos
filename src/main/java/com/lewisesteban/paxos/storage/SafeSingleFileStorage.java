package com.lewisesteban.paxos.storage;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class SafeSingleFileStorage implements StorageUnit {

    private FileManager fileManager;
    private TreeMap<String, String> content = null;

    public SafeSingleFileStorage(String fileName, FileAccessorCreator fileAccessorCreator) {
        fileManager = new FileManager(fileName, fileAccessorCreator);
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
            try {
                ObjectOutputStream oos = new ObjectOutputStream(writer);
                oos.writeObject(content);
                oos.flush();
            } catch (IOException e) {
                throw new StorageException(e);
            }
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

    private void readAllContent() throws IOException {
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

            } catch (EOFException | StreamCorruptedException e) {
                e.printStackTrace();
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

        } catch (ClassNotFoundException classNotFoundException) {
            throw new StorageException(classNotFoundException);
        }
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

        FileManager(String fileName, FileAccessorCreator fileAccessorCreator) {
            mainFile = fileAccessorCreator.create(fileName);
            tmpFile = fileAccessorCreator.create(fileName + "_tmp");
        }

        OutputStream startWrite() throws IOException {
            return tmpFile.startWrite();
        }

        void endWrite() throws IOException {
            tmpFile.endWrite();
            moveTempToMain();
        }

        InputStream startRead() throws IOException {
            if (!mainFile.exists() && tmpFile.exists()) {
                moveTempToMain();
            }
            try {
                return mainFile.startRead();
            } catch (FileNotFoundException e) {
                return null;
            }
        }

        void endRead() throws IOException {
            mainFile.endRead();
        }

        InputStream fixCorruption() throws IOException {
            endRead();
            if (tmpFile.exists()) {
                moveTempToMain();
                return mainFile.startRead();
            } else {
                return null;
            }
        }

        private void moveTempToMain() throws IOException {
            if (tmpFile.length() == 0)
                return;
            if (mainFile.exists()) {
                mainFile.delete();
            }
            Files.move(Paths.get(tmpFile.getFileName()), Paths.get(mainFile.getFileName()), StandardCopyOption.ATOMIC_MOVE);
        }

        void deleteAll() throws IOException {
            File folder = new File(".");
            File[] matchingFiles = folder.listFiles((dir, name) -> name.startsWith(mainFile.getFileName()));
            if (matchingFiles != null) {
                for (File file : matchingFiles) {
                    if (!file.delete()) {
                        throw new IOException("Could not delete " + file.getName());
                    }
                }
            }
        }

        void close() throws IOException {
            mainFile.endRead();
            mainFile.endWrite();
            tmpFile.endRead();
            tmpFile.endWrite();
        }
    }
}