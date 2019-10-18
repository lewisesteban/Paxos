package com.lewisesteban.paxos.storage;

import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class StorageTest extends TestCase {

    private String fileName(int nodeId) {
        return "storageTest" + nodeId;
    }

    public void testBasicSingleFileStorage() {
        try {
            new SafeSingleFileStorage(fileName(1), null, WholeFileAccessor.creator()).delete();
        } catch (IOException ignored) { }
        try {
            new SafeSingleFileStorage(fileName(2),null, WholeFileAccessor.creator()).delete();
        } catch (IOException ignored) { }
        try {
            new SafeSingleFileStorage(fileName(1), "test", WholeFileAccessor.creator()).delete();
        } catch (IOException ignored) { }
        try {
            new SafeSingleFileStorage(fileName(2),"test", WholeFileAccessor.creator()).delete();
        } catch (IOException ignored) { }
        testBasicSingleFileStorage(WholeFileAccessor.creator(), null);
        testBasicSingleFileStorage(WholeFileAccessor.creator(), "test");
        System.out.println("normal file accessor OK");
        testBasicSingleFileStorage(InterruptibleWholeFileAccessor.creator(true, 1), null);
        testBasicSingleFileStorage(InterruptibleWholeFileAccessor.creator(true, 1), "test");
        System.out.println("interruptible file accessor OK");
        testBasicSingleFileStorage(InterruptibleVirtualFileAccessor.creator(1), null);
        testBasicSingleFileStorage(InterruptibleVirtualFileAccessor.creator(1), "test");
        System.out.println("virtual file accessor OK");
    }

    private void testBasicSingleFileStorage(FileAccessorCreator fileAccessorCreator, String dir) {
        try {
            // write data into two files
            StorageUnit storage1 = new SafeSingleFileStorage(fileName(1), dir, fileAccessorCreator);
            StorageUnit storage2 = new SafeSingleFileStorage(fileName(2), dir, fileAccessorCreator);
            storage1.put("a", "A");
            storage1.flush();
            assertEquals("A", storage1.read("a"));
            assertNull(storage2.read("a"));
            storage2.put("b", "B");
            storage2.put("c", "c");
            storage2.flush();

            storage1.close();
            storage2.close();

            // read the two files from disk
            storage1 = new SafeSingleFileStorage(fileName(1), dir, fileAccessorCreator);
            storage2 = new SafeSingleFileStorage(fileName(2), dir, fileAccessorCreator);
            assertNull(storage1.read("b"));
            assertEquals("A", storage1.read("a"));
            Iterator<Map.Entry<String, String>> it = storage2.startReadAll();
            assertTrue(it.hasNext());
            assertEquals("B", it.next().getValue());
            assertTrue(it.hasNext());
            assertEquals("c", it.next().getKey());
            assertFalse(it.hasNext());

            // change data
            storage2.put("b", "B2");
            storage2.flush();
            assertEquals("B2", storage2.read("b"));

            // check delete
            storage2.delete();
            storage2.close();
            storage2 = new SafeSingleFileStorage(fileName(2), dir, fileAccessorCreator);
            assertNull(storage2.read("b"));

            // write a lot of data and then read from disk
            for (int i = 0; i <= InterruptibleWholeFileAccessor.FAST_WRITING_MAX + 1; ++i) {
                storage2.put("key" + i, "val" + i);
            }
            storage2.flush();
            storage2.close();
            storage2 = new SafeSingleFileStorage(fileName(2), dir, fileAccessorCreator);
            assertEquals("val0", storage2.read("key0"));
            assertEquals("val" + InterruptibleWholeFileAccessor.FAST_WRITING_MAX, storage2.read("key" + InterruptibleWholeFileAccessor.FAST_WRITING_MAX));
            assertEquals("val" + (InterruptibleWholeFileAccessor.FAST_WRITING_MAX + 1), storage2.read("key" + (InterruptibleWholeFileAccessor.FAST_WRITING_MAX + 1)));

            // clean-up
            storage1.delete();
            storage1.close();
            storage2.delete();
            storage2.close();

        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    private static boolean isCause(Class<? extends Throwable> expected, Throwable exc) {
        return expected.isInstance(exc) || (exc != null && isCause(expected, exc.getCause()));
    }

    public void testInterruptibleStorage() throws IOException {
        int size = 10000;
        int nbTests = 20;

        long slow = 0;
        for (int i = 0; i < nbTests; ++i)
            slow += testInterruptibleStorage(false, size);
        System.out.println("slow " + slow);

        long fast = 0;
        int allow = nbTests / 4;
        int nbRoundRes = 0;
        for (int i = 0; i < nbTests; ++i) {
            long written = testInterruptibleStorage(true, size);
            if (written % size == 0) {
                nbRoundRes++;
            }
            if (nbRoundRes > allow)
                fail();
            fast += written;
        }
        System.out.println("fast " + fast);
        System.out.println("nb of round results = " + nbRoundRes);

        if (fast < 100 * slow)
            fail();
    }

    private long testInterruptibleStorage(boolean fastWriting, int size) throws IOException {
        byte[] writingContent = new byte[size];
        for (int i = 0; i < writingContent.length; i++) {
            writingContent[i] = 42;
        }
        final File file = new File(fileName(1));
        //noinspection ResultOfMethodCallIgnored
        file.delete();
        FileAccessor fileAccessor = new InterruptibleWholeFileAccessor(fileName(1), null, fastWriting, 1);
        try {
            final OutputStream outputStream = fileAccessor.startWrite();
            AtomicReference<IOException> error = new AtomicReference<>(null);
            Thread worker = new Thread(() -> {
                try {
                    for (int time = 0; time < 10000; ++time) {
                        outputStream.write(writingContent);
                    }
                } catch (IOException e) {
                    if (!isCause(StorageInterruptedException.class, e)) {
                        error.set(e);
                    }
                }
            });

            worker.start();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            InterruptibleAccessorContainer.interrupt(1);

            if (error.get() != null) {
                error.get().printStackTrace();
                fail();
            }

            long written = file.length();
            if (!file.delete())
                fail();

            return written;

        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        return -1;
    }

    public void testFailSafeSingleFileStorage() throws StorageException, InterruptedException {
        // TODO this one sometimes fails: assertEquals("val", newInstance.read("test")); exepcted val got null
        testFailSafeSingleFileStorage(InterruptibleWholeFileAccessor.creator(false, 1), 15);
        testFailSafeSingleFileStorage(InterruptibleVirtualFileAccessor.creator(1), 1);
    }

    public void testFailSafeSingleFileStorage(FileAccessorCreator fileAccessorCreator, int waitTime) throws InterruptedException, StorageException {
        SafeSingleFileStorage fileDeleter = new SafeSingleFileStorage(fileName(1), null, fileAccessorCreator);
        try {
            fileDeleter.delete();
            fileDeleter.close();
        } catch (IOException ignored) { }

        for (int testnb = 0; testnb < 200; testnb++) {

            System.out.println("--- NEW TEST " + testnb);

            StorageUnit storageUnit = new SafeSingleFileStorage(fileName(1), null, fileAccessorCreator);
            try {
                assertNull(storageUnit.read("test"));
                storageUnit.put("test", "val");
                storageUnit.flush();
            } catch (IOException e) {
                e.printStackTrace();
                fail();
            }
            AtomicBoolean failed = new AtomicBoolean(false);
            AtomicInteger nbWrites = new AtomicInteger(0);
            Thread thread = new Thread(() -> {
                for (int i = 0; i < 1000000; ++i) {
                    try {
                        storageUnit.put(Integer.toString(i), "a");
                        storageUnit.flush();
                        nbWrites.incrementAndGet();
                        storageUnit.read(Integer.toString(i));
                    } catch (StorageException e) {
                        if (!isCause(StorageInterruptedException.class, e)) {
                            e.printStackTrace();
                            failed.set(true);
                        }
                        break;
                    }
                }
            });
            thread.start();
            Thread.sleep(waitTime);
            InterruptibleAccessorContainer.interrupt(1);
            System.out.println("# interrupted");
            int writeCount1 = nbWrites.get();
            System.out.println(writeCount1);
            Thread.sleep(waitTime);
            if (nbWrites.get() > writeCount1 + 1)
                fail();
            if (failed.get())
                fail();

            SafeSingleFileStorage newInstance = new SafeSingleFileStorage(fileName(1), null, fileAccessorCreator);
            try {
                assertEquals("val", newInstance.read("test"));
                newInstance.delete();
                newInstance.close();
            } catch (IOException e) {
                e.printStackTrace();
                fail();
            }
        }
    }

}
