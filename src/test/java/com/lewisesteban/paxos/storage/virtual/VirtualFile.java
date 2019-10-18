package com.lewisesteban.paxos.storage.virtual;

import java.io.*;
import java.util.ArrayList;

class VirtualFile {
    private ArrayList<Byte> content;

    VirtualFile() {
        this.content = new ArrayList<>();
    }

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
            content.add((byte)b);
        }

        @Override
        public void write(byte[] arr) {
            for (byte b : arr) {
                write(b);
            }
        }

        @Override
        public void write(byte[] arr, int off, int len) {
            for (int i = 0; i < len; ++i) {
                write(arr[i + off]);
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
        public int read(byte[] b) throws IOException {
            if (cursor >= content.size()) {
                throw new EOFException();
            }
            int i = 0;
            while (i < b.length && cursor + i < content.size()) {
                b[i] = content.get(cursor + i);
                ++i;
            }
            cursor += i;
            return i;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (cursor >= content.size()) {
                throw new EOFException();
            }
            int i = 0;
            while (i < len && cursor + i < content.size()) {
                b[i + off] = content.get(cursor + i);
                ++i;
            }
            cursor += i;
            return i;
        }

        @Override
        public long skip(long n) {
            if (cursor + n > content.size()) {
                n = content.size() - cursor;
                cursor = content.size();
            } else {
                cursor += n;
            }
            return n;
        }

        @Override
        public int available() {
            return content.size() - cursor;
        }

        @Override
        public void close() { }

        @Override
        public int read() throws IOException {
            if (cursor >= content.size()) {
                throw new EOFException();
            }
            return content.get(cursor++);
        }
    }
}
