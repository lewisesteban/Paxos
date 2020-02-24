package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.ClusterHandle;
import com.lewisesteban.paxos.paxosnode.Command;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

class ScatterManager {
    static int MAX_CALLS_PER_NODE = 10;
    static int QUEUE_MAX_SIZE = 100;

    private ClusterHandle memberList;
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Node[] nodes = null;
    private boolean initialized = false;

    ScatterManager(ClusterHandle memberList) {
        this.memberList = memberList;
    }

    private synchronized void init() {
        if (nodes == null) {
            nodes = new Node[memberList.getNbMembers()];
            for (int i = 0; i < nodes.length; ++i) {
                nodes[i] = new Node(i);
            }
        }
        initialized = true;
    }

    void sendCommand(int nodeId, long instanceId, Command command) {
        if (!initialized)
            init();
        nodes[nodeId].addTask(new ScatterTask(instanceId, command));
    }

    private class Node {
        private int nodeId;
        private Queue<ScatterTask> pendingTasks = new LinkedList<>();
        private AtomicInteger runningTasks = new AtomicInteger(0);

        private Node(int nodeId) {
            this.nodeId = nodeId;
            new Thread(this::backgroundWork).start();
        }

        private synchronized void backgroundWork() {
            while (true) {
                while (runningTasks.get() < MAX_CALLS_PER_NODE && !pendingTasks.isEmpty()) {
                    doTask(pendingTasks.remove());
                }
                try {
                    wait();
                } catch (InterruptedException ignored) {
                }
            }
        }

        private synchronized void doTask(ScatterTask task) {
            runningTasks.incrementAndGet();
            executor.submit(() -> {
                try {
                    memberList.getMembers().get(nodeId).getListener().execute(task.instanceId, task.command);
                } catch (Exception ignored) {
                }
                runningTasks.decrementAndGet();
                synchronized (Node.this) {
                    Node.this.notify();
                }
            });
        }

        private synchronized void addTask(ScatterTask task) {
            if (pendingTasks.size() == QUEUE_MAX_SIZE)
                pendingTasks.remove();
            pendingTasks.add(task);
            notify();
        }
    }

    private static class ScatterTask {
        private long instanceId;
        private Command command;

        private ScatterTask(long instanceId, Command command) {
            this.instanceId = instanceId;
            this.command = command;
        }
    }
}
