package com.lewisesteban.paxos.storage;

import sun.awt.Mutex;

import java.io.*;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;

public class InterruptibleWholeFileAccessor implements FileAccessor, InterruptibleStorage {

    static final int SLOW_WRITING_MAX = 10;
    static final int FAST_WRITING_MIN = 500;
    static final int FAST_WRITING_MAX = 1000;

    private File file;
    private InterruptibleOutputStream outputStream = null;
    private FileInputStream inputStream = null;
    private boolean fastWriting;
    private boolean interrupted = false;
    private Mutex writing = new Mutex();
    private Mutex reading = new Mutex();
    private int nodeId;

    InterruptibleWholeFileAccessor(String name, String dirName, boolean fastWriting, int nodeId) throws StorageException {
        this.fastWriting = fastWriting;
        if (dirName != null && !dirName.equals(".")) {
            File dir = new File(dirName);
            if (!dir.exists()) {
                if (!dir.mkdir())
                    throw new StorageException("Failed to create directory");
            }
            file = new File(dir + File.separator + name);
        } else {
            file = new File(name);
        }
        InterruptibleAccessorContainer.add(nodeId, this);
        this.nodeId = nodeId;
    }

    public void interrupt() {
        interrupted = true;
        writing.lock();
        reading.lock();
        if (outputStream != null) {
            try {
                outputStream.interruptClose();
                outputStream = null;
            } catch (IOException ignored) { }
        }
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException ignored) { }
        }
        reading.unlock();
        writing.unlock();
    }

    @Override
    public OutputStream startWrite() throws StorageException {
        endRead();
        writing.lock();
        try {
            if (interrupted) {
                throw new StorageInterruptedException();
            }
            InterruptibleOutputStream outputStream = new InterruptibleOutputStream(file.getPath());
            this.outputStream = outputStream;
            return outputStream;
        } catch (FileNotFoundException e) {
            throw new StorageException(e);
        } finally {
            writing.unlock();
        }
    }

    @Override
    public void endWrite() throws StorageException {
        if (outputStream != null) {
            writing.lock();
            try {
                if (interrupted) {
                    throw new StorageInterruptedException();
                }
                try {
                    outputStream.close();
                    outputStream = null;
                } catch (IOException e) {
                    throw new StorageException(e);
                }
            } finally {
                writing.unlock();
            }
        }
    }

    @Override
    public InputStream startRead() throws StorageException {
        reading.lock();
        try {
            if (interrupted) {
                throw new StorageInterruptedException();
            }
            endWrite();
            try {
                inputStream = new FileInputStream(file.getPath());
            } catch (FileNotFoundException e) {
                throw new StorageException(e);
            }
            return inputStream;
        } finally {
            if (inputStream == null) {
                reading.unlock();
            }
        }
    }

    @Override
    public void endRead() throws StorageException {
        if (inputStream != null) {
            try {
                inputStream.close();
                inputStream = null;
                reading.unlock();
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
        if (interrupted) {
            throw new StorageInterruptedException();
        }
    }

    @Override
    public void delete() throws StorageException {
        writing.lock();
        try {
            if (interrupted) {
                throw new StorageInterruptedException();
            }
            if (!file.delete())
                throw new StorageException("Could not delete " + file.getPath());
        } finally {
            writing.unlock();
        }
    }

    @Override
    public boolean exists() {
        return file.exists();
    }

    @Override
    public long length() {
        return file.length();
    }

    @Override
    public void moveTo(FileAccessor dest, CopyOption copyOption) throws StorageException {
        writing.lock();
        try {
            if (interrupted)
                throw new StorageInterruptedException();
            Files.move(Paths.get(getFilePath()), Paths.get(dest.getFilePath()), copyOption);
        } catch (IOException e) {
            throw new StorageException(e);
        } finally {
            writing.unlock();
        }
    }

    @Override
    public String getFilePath() {
        return file.getPath();
    }

    @Override
    public String getName() {
        return file.getName();
    }

    @Override
    public FileAccessor[] listFiles() throws StorageException {
        File[] files = file.listFiles();
        if (files == null)
            return null;
        FileAccessor[] accessors = new FileAccessor[files.length];
        for (int i = 0; i < accessors.length; ++i) {
            accessors[i] = new InterruptibleWholeFileAccessor(files[i].getName(), file.getName(), fastWriting, nodeId);
        }
        return accessors;
    }

    class InterruptibleOutputStream extends OutputStream {

        FileOutputStream outputStream;
        String fileName;
        Random random = new Random();

        InterruptibleOutputStream(String fileName) throws FileNotFoundException, StorageInterruptedException {
            if (interrupted)
                throw new StorageInterruptedException();
            this.fileName = fileName;
            outputStream = new FileOutputStream(fileName);
        }

        @Override
        public void write(int b) throws IOException {
            writing.lock();
            try {
                if (interrupted)
                    throw new StorageInterruptedException();
                outputStream.write(b);
                outputStream.flush();
            } finally {
                writing.unlock();
            }
        }

        @Override
        public void write(byte[] arr) throws IOException {
            writing.lock();
            try {
                int i = 0;
                while (i < arr.length) {
                    if (interrupted)
                        throw new StorageInterruptedException();
                    int flushFreq =
                            fastWriting ? FAST_WRITING_MIN + random.nextInt(FAST_WRITING_MAX - FAST_WRITING_MIN)
                                    : random.nextInt(SLOW_WRITING_MAX);
                    int to = i + flushFreq;
                    if (to > arr.length)
                        to = arr.length;
                    byte[] piece = Arrays.copyOfRange(arr, i, to);
                    outputStream.write(piece);
                    outputStream.flush();
                    i += flushFreq;
                }
            } finally {
                writing.unlock();
            }
        }

        public void close() throws IOException {
            if (interrupted) {
                throw new StorageInterruptedException();
            }
            if (outputStream != null) {
                outputStream.close();
                outputStream = null;
            }
        }

        void interruptClose() throws IOException {
            if (outputStream != null) {
                outputStream.close();
                outputStream = null;
            }
        }
    }

    public static FileAccessorCreator creator(boolean fastWriting, int nodeId) {
        return (fileName, dir) -> new InterruptibleWholeFileAccessor(fileName, dir, fastWriting, nodeId);
    }
}
