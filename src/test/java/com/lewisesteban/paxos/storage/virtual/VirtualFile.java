package com.lewisesteban.paxos.storage.virtual;

import java.io.*;
import java.util.ArrayList;

class VirtualFile {
    private final ArrayList<Byte> content = new ArrayList<>();

    VirtualFile() { }

    long getLength() {
        return content.size();
    }

    InputStream getInputStream() {
        return new VirtualFileInputStream();
    }

    OutputStream getOutputStream() {
        return new VirtualFileOutputStream();
    }

    class VirtualFileOutputStream extends OutputStream {

        @Override
        public void write(int b) {
            synchronized (content) {
                content.add((byte) b);
            }
        }

        @Override
        public void write(byte[] arr) {
            synchronized (content) {
                for (byte b : arr) {
                    write(b);
                }
            }
        }

        @Override
        public void write(byte[] arr, int off, int len) {
            synchronized (content) {
                for (int i = 0; i < len; ++i) {
                    write(arr[i + off]);
                }
            }
        }

        @Override
        public void flush() { }

        @Override
        public void close() { }
    }

    class VirtualFileInputStream extends InputStream {
        int cursor = 0;

        @Override
        public int read(byte[] b) {
            synchronized (content) {
                if (cursor >= content.size()) {
                    return -1;
                }
                int i = 0;
                while (i < b.length && cursor + i < content.size()) {
                    b[i] = content.get(cursor + i);
                    ++i;
                }
                cursor += i;
                return i;
            }
        }

        @Override
        public int read(byte[] b, int off, int len) {
            synchronized (content) {
                if (cursor >= content.size()) {
                    return -1;
                }
                int i = 0;
                while (i < len && cursor + i < content.size() && off + i < b.length) {
                    b[i + off] = content.get(cursor + i);
                    ++i;
                }
                cursor += i;
                return i;
            }
        }

        @Override
        public long skip(long n) {
            synchronized (content) {
                if (cursor + n > content.size()) {
                    n = content.size() - cursor;
                    cursor = content.size();
                } else {
                    cursor += n;
                }
                return n;
            }
        }

        @Override
        public int available() {
            synchronized (content) {
                return content.size() - cursor;
            }
        }

        @Override
        public void close() { }

        @Override
        public int read() {
            synchronized (content) {
                if (cursor >= content.size()) {
                    return -1;
                }
                return content.get(cursor++);
            }
        }
    }
}
