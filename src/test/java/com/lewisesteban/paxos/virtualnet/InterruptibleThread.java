package com.lewisesteban.paxos.virtualnet;


/**
 * A thread with a simple "interrupted" flag.
 */
public class InterruptibleThread extends Thread {
    private boolean interrupted = false;

    public InterruptibleThread(Runnable target) {
        super(target);
    }

    @Override
    public boolean isInterrupted() {
        return interrupted;
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }
}
