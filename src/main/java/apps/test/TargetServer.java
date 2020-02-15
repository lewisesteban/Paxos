package apps.test;

import java.util.Random;
import java.util.concurrent.Semaphore;

public class TargetServer implements Target {
    private TesterServer server;
    private GUIServerPanel panel;
    private Semaphore semaphore = new Semaphore(1);
    private Random random = new Random();
    private int lastStartTime = 0;

    TargetServer(TesterServer server, GUIServerPanel panel) {
        this.server = server;
        this.panel = panel;
    }

    @Override
    public boolean isAlive() {
        return server.isUp();
    }

    @Override
    public boolean kill() {
        if (semaphore.tryAcquire()) {
            if (server.isUp()) {
                new Thread(() -> {
                    // slow down killing of servers to make it similar to starting
                    try {
                        int rand;
                        if (lastStartTime == 0)
                            rand = 0;
                        else
                            rand = random.nextInt(lastStartTime / 3) - (lastStartTime / 6);
                        Thread.sleep(lastStartTime + rand);
                    } catch (InterruptedException ignored) { }
                    panel.onOffBtnClick();
                    semaphore.release();
                }).start();
            } else {
                semaphore.release();
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean restore() {
        if (semaphore.tryAcquire()) {
            if (!server.isUp()) {
                new Thread(() -> {
                    long start = System.currentTimeMillis();
                    panel.onOffBtnClick();
                    lastStartTime = (int)(System.currentTimeMillis() - start);
                    semaphore.release();
                }).start();
            } else {
                semaphore.release();
            }
            return true;
        }
        return false;
    }
}
