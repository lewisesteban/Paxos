package com.lewisesteban.paxos.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class InterruptibleTestStorage implements StorageUnit {

    private StorageUnit baseStorage;
    private boolean running;
    private ExecutorService executor;
    private Thread thread;

    public InterruptibleTestStorage(int nodeId, StorageUnit baseStorage) {
        this.baseStorage = baseStorage;
        start();
        Container.add(nodeId, this);
    }

    private synchronized void start() {
        running = true;
        ThreadFactory factory = r -> {
            thread = new Thread(r);
            return thread;
        };
        executor = Executors.newSingleThreadExecutor(factory);
    }

    public void interrupt() {
        running = false;
        executor.shutdownNow();
        try {
            if (thread != null)
                thread.join();
        } catch (InterruptedException ignored) { }
        try {
            baseStorage.close();
        } catch (IOException ignored) { }
        System.gc();
    }

    public synchronized void delete() throws StorageException {
        baseStorage.delete();
    }

    @Override
    public synchronized Iterator<Map.Entry<String, String>> startReadAll() throws StorageException {
        if (!running)
            throw new StorageException("interrupted");
        try {
            return executor.submit(() -> baseStorage.startReadAll()).get();
        } catch (ExecutionException e) {
            if (running)
                throw new StorageException(e);
            else
                return null;
        } catch (InterruptedException | RejectedExecutionException e) {
            return null;
        }
    }

    @Override
    public synchronized String read(String key) throws StorageException {
        if (!running)
            throw new StorageException("interrupted");
        try {
            return executor.submit(() -> baseStorage.read(key)).get();
        } catch (ExecutionException e) {
            if (running)
                throw new StorageException(e);
            else
                return null;
        } catch (InterruptedException | RejectedExecutionException e) {
            return null;
        }
    }

    @Override
    public synchronized void put(String key, String value) throws StorageException {
        if (!running)
            throw new StorageException("interrupted");
        AtomicReference<IOException> error = new AtomicReference<>(null);
        try {
            executor.submit(() -> {
                try {
                    baseStorage.put(key, value);
                } catch (IOException e) {
                    error.set(e);
                }
            }).get();
        } catch (ExecutionException e) {
            throw new StorageException(e);
        } catch (InterruptedException | RejectedExecutionException ignored) { }
        if (running && error.get() != null) {
            throw new StorageException(error.get());
        }
    }

    @Override
    public synchronized void flush() throws StorageException {
        if (!running)
            throw new StorageException("interrupted");
        AtomicReference<IOException> error = new AtomicReference<>(null);
        try {
            executor.submit(() -> {
                try {
                    baseStorage.flush();
                } catch (IOException e) {
                    error.set(e);
                }
            }).get();
        } catch (ExecutionException e) {
            throw new StorageException(e);
        } catch (InterruptedException | RejectedExecutionException ignored) { }
        if (running && error.get() != null) {
            throw new StorageException(error.get());
        }
    }

    @Override
    public synchronized void close() throws IOException {
        baseStorage.close();
    }

    public static class Container {
        private static Map<Integer, InterruptibleTestStorage> map = new HashMap<>();

        static void add(int nodeId, InterruptibleTestStorage ref) {
            map.put(nodeId, ref);
        }

        public static InterruptibleTestStorage get(int nodeId) {
            return map.get(nodeId);
        }

        public static void deleteAllFiles() {
            for (InterruptibleTestStorage storage : map.values()) {
                try {
                    storage.delete();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            map.clear();
        }
    }
}
