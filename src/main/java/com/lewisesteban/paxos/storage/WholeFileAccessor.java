package com.lewisesteban.paxos.storage;

import java.io.*;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Paths;

public class WholeFileAccessor implements FileAccessor {

    private File file;
    private FileOutputStream outputStream = null;
    private FileInputStream inputStream = null;

    public WholeFileAccessor(String name, String dirName) throws StorageException {
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
    }

    @Override
    public OutputStream startWrite() throws StorageException {
        try {
            endRead();
            outputStream = new FileOutputStream(file.getPath());
            return outputStream;
        } catch (FileNotFoundException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void endWrite() throws StorageException {
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
    }

    @Override
    public InputStream startRead() throws StorageException {
        try {
            endWrite();
            inputStream = new FileInputStream(file.getPath());
            return inputStream;
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void endRead() throws StorageException {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                throw new StorageException(e);
            }
        }
    }

    @Override
    public void delete() throws StorageException {
        if (!file.delete())
            throw new StorageException("Could not delete " + file.getPath());
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

    @Override
    public String getName() {
        return file.getName();
    }

    @Override
    public void moveTo(FileAccessor dest, CopyOption copyOption) throws StorageException {
        try {
            Files.move(Paths.get(getFilePath()), Paths.get(dest.getFilePath()), copyOption);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public FileAccessor[] listFiles() throws StorageException {
        File[] files = file.listFiles();
        if (files == null)
            return null;
        FileAccessor[] accessors = new FileAccessor[files.length];
        for (int i = 0; i < accessors.length; ++i) {
            accessors[i] = new WholeFileAccessor(files[i].getName(), file.getName());
        }
        return accessors;
    }

    static FileAccessorCreator creator() {
        return WholeFileAccessor::new;
    }
}
