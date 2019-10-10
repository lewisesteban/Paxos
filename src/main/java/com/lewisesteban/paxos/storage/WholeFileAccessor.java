package com.lewisesteban.paxos.storage;

import java.io.*;

public class WholeFileAccessor implements FileAccessor {

    private File file;
    private FileOutputStream outputStream = null;
    private FileInputStream inputStream = null;

    private WholeFileAccessor(String name) {
        file = new File(name);
    }

    @Override
    public OutputStream startWrite() throws IOException {
        endRead();
        outputStream = new FileOutputStream(file.getName());
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
        inputStream = new FileInputStream(file.getName());
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
            throw new IOException("Could not delete " + file.getName());
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
    public String getFileName() {
        return file.getName();
    }

    public static FileAccessorCreator creator() {
        return WholeFileAccessor::new;
    }
}
