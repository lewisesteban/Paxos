package com.lewisesteban.paxos.storage.virtual;

import com.lewisesteban.paxos.storage.StorageException;

import java.io.InputStream;
import java.io.OutputStream;

class VirtualFileHandle {
    private String path;

    VirtualFileHandle(String path) {
        this.path = path;
    }

    String getPath() {
        return path;
    }

    boolean exists() {
        return VirtualFileSystem.exists(path);
    }

    boolean delete() {
        if (!exists())
            return false;
        VirtualFileSystem.delete(path);
        return true;
    }

    long length() throws StorageException {
        VirtualFile file = VirtualFileSystem.get(path);
        if (file == null)
            throw new StorageException("File doesn't exist");
        return file.getLength();
    }

    OutputStream getOutputStream() {
        VirtualFile file = VirtualFileSystem.get(path);
        if (file == null)
            file = VirtualFileSystem.create(path);
        return file.getOutputStream();
    }

    InputStream getInputStream() {
        VirtualFile virtualFile = VirtualFileSystem.get(path);
        if (virtualFile == null)
            return null;
        return virtualFile.getInputStream();
    }
}
