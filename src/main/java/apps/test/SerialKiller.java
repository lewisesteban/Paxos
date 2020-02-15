package apps.test;

import java.util.List;
import java.util.Random;

class SerialKiller {
    private List<Target> targets;
    private boolean keepGoing = false;
    private Random random = new Random();
    private int nbAlive;
    private Thread thread = null;

    SerialKiller(List<Target> targets) {
        this.targets = targets;
    }

    synchronized void startInBackground(int minWait, int maxWait) {
        thread = new Thread(() -> work(minWait, maxWait));
        keepGoing = true;
        thread.start();
    }

    synchronized void stop() {
        keepGoing = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            thread = null;
        }
    }

    synchronized boolean isGoing() {
        return keepGoing;
    }

    private void work(int minWait, int maxWait) {
        nbAlive = 0;
        targets.forEach(target -> { if (target.isAlive()) nbAlive++; });
        boolean restoring = nbAlive <= targets.size() / 2;
        while (keepGoing) {
            if (shouldChange(restoring))
                restoring = !restoring;
            boolean doOpposite = (random.nextInt(4) == 0);
            try {
                affectRandom(doOpposite != restoring);
            } catch (Exception e) {
                restoring = !restoring;
            } finally {
                try {
                    if (minWait >= maxWait)
                        Thread.sleep(minWait);
                    else
                        Thread.sleep(minWait + random.nextInt(maxWait - minWait));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean affectRandom(boolean restore) throws Exception {
        int i = random.nextInt(targets.size());
        int nbAttempts = 0;
        while (!((restore && !targets.get(i).isAlive()) || (!restore && targets.get(i).isAlive()))) {
            i = (i + 1) % targets.size();
            nbAttempts++;
            if (nbAttempts == targets.size()) {
                if (restore)
                    nbAlive = targets.size();
                else
                    nbAlive = 0;
                throw new Exception("No valid target");
            }
        }
        if (restore) {
            if (targets.get(i).restore()) {
                nbAlive++;
                return true;
            } else {
                return false;
            }
        } else {
            if (targets.get(i).kill()) {
                nbAlive--;
                return true;
            } else {
                return false;
            }
        }
    }

    private boolean shouldChange(boolean restoring) {
        if (restoring) {
            if (nbAlive == targets.size()) {
                return true;
            } else if (nbAlive > targets.size() / 2) {
                int rand = random.nextInt(targets.size() - nbAlive + 1); // the +1 is to keep them alive longer
                return rand == 0;
            } else {
                return false;
            }
        } else {
            if (nbAlive == 0) {
                return true;
            } else if (nbAlive <= targets.size() / 2) {
                int rand = random.nextInt(nbAlive);
                return rand == 0;
            } else {
                return false;
            }
        }
    }
}
