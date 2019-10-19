package com.lewisesteban.paxos.storage.virtual;

import com.lewisesteban.paxos.storage.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.CopyOption;
import java.nio.file.Paths;
import java.util.List;

public class InterruptibleVirtualFileAccessor implements FileAccessor, InterruptibleStorage {

    private VirtualFileHandle file;
    private boolean interrupted = false;
    private String name;
    private int nodeId;

    InterruptibleVirtualFileAccessor(String name, String dirName, int nodeId) {
        this.nodeId = nodeId;
        this.name = name;
        if (dirName != null && !dirName.equals(".")) {
            file = new VirtualFileHandle(dirName + File.separator + name);
        } else {
            file = new VirtualFileHandle(name);
        }
        InterruptibleAccessorContainer.add(nodeId, this);
    }

    InterruptibleVirtualFileAccessor(String path, int nodeId) {
        this.nodeId = nodeId;
        this.name = Paths.get(path).getFileName().toString();
        file = new VirtualFileHandle(path);
        InterruptibleAccessorContainer.add(nodeId, this);
    }

    public void interrupt() {
        interrupted = true;
    }

    @Override
    public OutputStream startWrite() throws StorageException {
        if (interrupted) {
            throw new StorageInterruptedException();
        }
        return new InterruptibleOutputStream(file);
    }

    @Override
    public void endWrite() throws StorageException {
        if (interrupted) {
            throw new StorageInterruptedException();
        }
    }

    @Override
    public InputStream startRead() throws StorageException {
        if (interrupted) {
            throw new StorageInterruptedException();
        }
        return file.getInputStream();
    }

    @Override
    public void endRead() throws StorageException {
        if (interrupted) {
            throw new StorageInterruptedException();
        }
    }

    @Override
    public void delete() throws StorageException {
        if (interrupted) {
            throw new StorageInterruptedException();
        }
        file.delete();
    }

    @Override
    public boolean exists() {
        return file.exists();
    }

    @Override
    public long length() throws StorageException {
        return file.length();
    }

    @Override
    public String getFilePath() {
        return file.getPath();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void moveTo(FileAccessor dest, CopyOption copyOption) throws StorageException {
        if (interrupted)
            throw new StorageInterruptedException();
        VirtualFileSystem.move(file.getPath(), dest.getFilePath());
    }

    @Override
    public FileAccessor[] listFiles() {
        List<String> files = VirtualFileSystem.listFiles(getName());
        if (files == null)
            return null;
        FileAccessor[] accessors = new FileAccessor[files.size()];
        for (int i = 0; i < accessors.length; ++i) {
            accessors[i] = new InterruptibleVirtualFileAccessor(files.get(i), nodeId);
        }
        return accessors;
    }

    class InterruptibleOutputStream extends OutputStream {

        OutputStream outputStream;
        VirtualFileHandle fileHandle;

        InterruptibleOutputStream(VirtualFileHandle fileHandle) throws StorageInterruptedException {
            if (interrupted)
                throw new StorageInterruptedException();
            this.fileHandle = fileHandle;
            outputStream = fileHandle.getOutputStream();
        }

        @Override
        public void write(int b) throws IOException {
            if (interrupted)
                throw new StorageInterruptedException();
            outputStream.write(b);
            outputStream.flush();
        }

        @Override
        public void write(byte[] arr) throws IOException {
            for (byte b : arr) {
                write(b);
            }
        }

        public void close() throws IOException {
            if (interrupted) {
                throw new StorageInterruptedException();
            }
        }

    }

    public static FileAccessorCreator creator(int nodeId) {
        return (fileName, dir) -> new InterruptibleVirtualFileAccessor(fileName, dir, nodeId);
    }
}
