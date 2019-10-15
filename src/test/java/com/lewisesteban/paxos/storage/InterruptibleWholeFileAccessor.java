package com.lewisesteban.paxos.storage;

import java.io.*;
import java.util.Arrays;
import java.util.Random;

public class InterruptibleWholeFileAccessor implements FileAccessor {

    @SuppressWarnings("WeakerAccess")
    static final int FAST_WRITING_MIN = 500;
    static final int FAST_WRITING_MAX = 1000;

    private File file;
    private InterruptibleOutputStream outputStream = null;
    private FileInputStream inputStream = null;
    private boolean fastWriting;

    InterruptibleWholeFileAccessor(String name, String dirName, boolean fastWriting) throws IOException {
        this.fastWriting = fastWriting;
        if (dirName != null && !dirName.equals(".")) {
            File dir = new File(dirName);
            if (!dir.exists()) {
                if (!dir.mkdir())
                    throw new IOException("Failed to create directory");
            }
            file = new File(dir + File.separator + name);
        } else {
            file = new File(name);
        }
    }

    @Override
    public OutputStream startWrite() throws IOException {
        endRead();
        outputStream = new InterruptibleOutputStream(file.getPath());
        return outputStream;
    }

    @Override
    public void endWrite() throws IOException {
        if (outputStream != null) {
            outputStream.close();
        }
    }

    @Override
    public InputStream startRead() throws IOException {
        endWrite();
        inputStream = new FileInputStream(file.getPath());
        return inputStream;
    }

    @Override
    public void endRead() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    @Override
    public void delete() throws IOException {
        if (!file.delete())
            throw new IOException("Could not delete " + file.getPath());
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
    public String getFilePath() {
        return file.getPath();
    }

    class InterruptibleOutputStream extends OutputStream {

        FileOutputStream outputStream;
        String fileName;
        int flushFreq = 100 + new Random().nextInt(100);
        int counter = 0;

        InterruptibleOutputStream(String fileName) throws FileNotFoundException {
            this.fileName = fileName;
            outputStream = new FileOutputStream(fileName);
        }

        @Override
        public void write(int b) throws IOException {
            if (Thread.interrupted())
                throw new IOException("interrupted");
            outputStream.write(b);
            if (fastWriting) {
                counter++;
                if (counter == flushFreq) {
                    outputStream.flush();
                    counter = 0;
                }
            } else {
                outputStream.flush();
            }
        }

        public void write(byte[] arr) throws IOException {
            if (fastWriting) {
                int i = 0;
                while (i < arr.length) {
                    if (Thread.interrupted())
                        throw new IOException("interrupted");
                    int flushFreq = FAST_WRITING_MIN + new Random().nextInt(FAST_WRITING_MAX - FAST_WRITING_MIN);
                    int to = i + flushFreq;
                    if (to > arr.length)
                        to = arr.length;
                    byte[] piece = Arrays.copyOfRange(arr, i, to);
                    outputStream.write(piece);
                    outputStream.flush();
                    i += flushFreq;
                }
            } else {
                for (byte b : arr) {
                    write(b);
                }
            }
        }

        public void close() throws IOException {
            outputStream.close();
        }
    }

    public static FileAccessorCreator creator(boolean fastWriting) {
        return (fileName, dir) -> new InterruptibleWholeFileAccessor(fileName, dir, fastWriting);
    }
}
