package com.lewisesteban.paxos.storage;

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
            StorageUnit storage1 = new SafeSingleFileStorage(fileName(1), WholeFileAccessor.creator());
            StorageUnit storage2 = new SafeSingleFileStorage(fileName(2), WholeFileAccessor.creator());
            storage1.write("a", "A");
            assertEquals("A", storage1.read("a"));
            assertNull(storage2.read("a"));
            storage2.write("b", "B");
            storage2.write("c", "c");

            storage1.close();
            storage2.close();

            storage1 = new SafeSingleFileStorage(fileName(1), WholeFileAccessor.creator());
            storage2 = new SafeSingleFileStorage(fileName(2), WholeFileAccessor.creator());
            assertNull(storage1.read("b"));
            assertEquals("A", storage1.read("a"));
            Iterator<Map.Entry<String, String>> it = storage2.startReadAll();
            assertTrue(it.hasNext());
            assertEquals("B", it.next().getValue());
            assertTrue(it.hasNext());
            assertEquals("c", it.next().getKey());
            assertFalse(it.hasNext());

            storage2.write("b", "B2");
            assertEquals("B2", storage2.read("b"));

            storage2.delete();
            storage2.close();
            storage2 = new SafeSingleFileStorage(fileName(2), WholeFileAccessor.creator());
            assertNull(storage2.read("b"));

            storage1.delete();
            storage1.close();
            storage2.delete();
            storage2.close();

        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    public void testInterruptibleStorage() {
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

    private long testInterruptibleStorage(boolean fastWriting, int size) {
        byte[] writingContent = new byte[size];
        for (int i = 0; i < writingContent.length; i++) {
            writingContent[i] = 42;
        }
        final File file = new File(fileName(1));
        //noinspection ResultOfMethodCallIgnored
        file.delete();
        FileAccessor fileAccessor = new InterruptibleWholeFileAccessor(fileName(1), fastWriting);
        try {
            final OutputStream outputStream = fileAccessor.startWrite();

            InterruptibleTestStorage testStorage = new InterruptibleTestStorage(1, new StorageUnit() {
                @Override
                public Iterator<Map.Entry<String, String>> startReadAll() {
                    return null;
                }

                @Override
                public String read(String key) {
                    return null;
                }

                @Override
                public void write(String key, String value) throws IOException {
                    for (int time = 0; time < 10000; ++time) {
                        outputStream.write(writingContent);
                    }
                }

                @Override
                public void delete() { }

                @Override
                public void close() { }
            });

            AtomicReference<IOException> error = new AtomicReference<>(null);
            Thread worker = new Thread(() -> {
                try {
                    testStorage.write(null, null);
                } catch (IOException e) {
                    if (e.getMessage() == null || !e.getMessage().equals("interrupted"))
                        error.set(e);
                }
            });

            worker.start();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            testStorage.interrupt();
            try {
                worker.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (error.get() != null) {
                error.get().printStackTrace();
                fail();
            }

            long written = file.length();
            outputStream.close();
            if (!file.delete())
                fail();

            return written;

        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        return -1;
    }

    public void testFailSafeSingleFileStorage() throws InterruptedException {
        SafeSingleFileStorage fileDeleter = new SafeSingleFileStorage(fileName(1), WholeFileAccessor.creator());
        try {
            fileDeleter.delete();
            fileDeleter.close();
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }

        for (int testnb = 0; testnb < 100; testnb++) {

            FileAccessorCreator fileAccessorCreator = InterruptibleWholeFileAccessor.creator(false);
            StorageUnit storageUnit = new SafeSingleFileStorage(fileName(1), fileAccessorCreator);
            InterruptibleTestStorage interruptibleStorage = new InterruptibleTestStorage(1, storageUnit);
            try {
                assertNull(interruptibleStorage.read("test"));
                interruptibleStorage.write("test", "val");
            } catch (IOException e) {
                e.printStackTrace();
                fail();
            }
            AtomicBoolean failed = new AtomicBoolean(false);
            AtomicInteger nbWrites = new AtomicInteger(0);
            Thread thread = new Thread(() -> {
                for (int i = 0; i < 1000000; ++i) {
                    try {
                        nbWrites.incrementAndGet();
                        interruptibleStorage.write(Integer.toString(i), "a");
                        interruptibleStorage.read(Integer.toString(i));
                    } catch (IOException e) {
                        if (!((e.getCause() != null && e.getCause().getMessage().equals("java.lang.ThreadDeath"))
                                || (e.getMessage() != null && e.getMessage().equals("interrupted")))) {
                            e.printStackTrace();
                            failed.set(true);
                        }
                        break;
                    }
                }
            });
            thread.start();
            Thread.sleep(20);
            interruptibleStorage.interrupt();
            int writeCount1 = nbWrites.get();
            System.out.println(writeCount1);
            Thread.sleep(20);
            if (nbWrites.get() > writeCount1)
                fail();
            if (failed.get())
                fail();

            InterruptibleTestStorage newInstance = new InterruptibleTestStorage(1, new SafeSingleFileStorage(fileName(1), WholeFileAccessor.creator()));
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
