package com.lewisesteban.paxos.storage;

import java.io.*;

public class WholeFileAccessor implements FileAccessor {

    private File file;
    private FileOutputStream outputStream = null;
    private FileInputStream inputStream = null;

    private WholeFileAccessor(String name, String dirName) throws IOException {
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
        outputStream = new FileOutputStream(file.getPath());
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

    public static FileAccessorCreator creator() {
        return WholeFileAccessor::new;
    }
}
