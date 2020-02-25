package apps.test;

import java.util.List;
import java.util.Random;

class SerialKiller {
    private List<Target> targets;
    private boolean keepGoing = false;
    private Random random = new Random();
    private int nbAlive;
    private Thread thread = null;
    private int objective;
    private boolean restoring;

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
        restoring = true;
        objective = targets.size();
        while (keepGoing) {
            if (nbAlive == objective) {
                changeObjective();
            }
            try {
                affectRandom(restoring);
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

    private void affectRandom(boolean restore) throws Exception {
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
            }
        } else {
            if (targets.get(i).kill()) {
                nbAlive--;
            }
        }
    }

    private void changeObjective() {
        if (restoring) {
            int rand = random.nextInt(100);
            int nbKilled;
            if (rand > 70) {
                nbKilled = 1;
            } else if (rand > 40) { // max=size/3
                nbKilled = random.nextInt(targets.size() / 3) + 1;
            } else if (rand > 20) { // max=size/3+1
                nbKilled = random.nextInt(targets.size() / 3 + 1) + 1;
            } else { // max=size
                nbKilled = random.nextInt(targets.size()) + 1;
            }
            objective = targets.size() - nbKilled;
            restoring = false;
        } else {
            objective = targets.size();
            restoring = true;
        }
    }
}
