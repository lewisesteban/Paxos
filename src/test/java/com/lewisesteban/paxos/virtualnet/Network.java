package com.lewisesteban.paxos.virtualnet;

import java.util.*;

public class Network {

    private Map<Integer, VirtualNetNode> nodes = new HashMap<>();
    private Set<Integer> isolatedRacks = new TreeSet<>();

    private int waitTimeMin = 10;
    private int waitTimeUsualMax = 30;
    private int waitTimeUnusualMax = 100;
    private float unusualWaitRisk = 0.1f;

    public boolean tryCall(int from, int to) {
        waitForAnswer();
        if (nodes.get(from).isRunning() && nodes.get(to).isRunning() && canCommunicate(from, to)) {
            System.out.println("Network call from " + from + " to " + to);
            return true;
        } else {
            System.out.println("FAILED Network call from " + from + " to " + to);
            return false;
        }
    }

    public void setWaitTimes(int waitTimeMin, int waitTimeUsualMax, int waitTimeUnusualMax, float unusualWaitRisk) {
        this.waitTimeMin = waitTimeMin;
        this.waitTimeUsualMax = waitTimeUsualMax;
        this.waitTimeUnusualMax = waitTimeUnusualMax;
        this.unusualWaitRisk = unusualWaitRisk;
    }

    public void addNode(VirtualNetNode node) {
        this.nodes.put(node.getAddress(), node);
    }

    public void start(int address) {
        nodes.get(address).start();
    }

    public void stop(int address) {
        nodes.get(address).shutDown();
    }

    public void kill(int address) {
        nodes.get(address).kill();
    }

    public void startAll() {
        for (VirtualNetNode node : nodes.values()) {
            node.start();
        }
    }

    public void stopAll() {
        for (VirtualNetNode node : nodes.values()) {
            node.shutDown();
        }
    }

    public void killAll() {
        for (VirtualNetNode node : nodes.values()) {
            node.kill();
        }
    }

    public void startRack(int rack) {
        for (VirtualNetNode node : nodes.values()) {
            if (node.getRack() == rack) {
                node.start();
            }
        }
    }

    public void stopRack(int rack) {
        for (VirtualNetNode node : nodes.values()) {
            if (node.getRack() == rack) {
                node.shutDown();
            }
        }
    }

    public void killRack(int rack) {
        for (VirtualNetNode node : nodes.values()) {
            if (node.getRack() == rack) {
                node.kill();
            }
        }
    }

    public boolean isRackConnected(int rack) {
        return !isolatedRacks.contains(rack);
    }

    public void disconnectRack(int rack) {
        isolatedRacks.add(rack);
    }

    public void reconnectRack(int rack) {
        isolatedRacks.remove(rack);
    }

    private boolean canCommunicate(int node1, int node2) {
        int rack1 = nodes.get(node1).getRack();
        int rack2 = nodes.get(node2).getRack();
        return ((rack1 == rack2) || (isRackConnected(rack1) && isRackConnected(rack2)));
    }

    private void waitForAnswer() {
        int wait;
        Random rand = new Random();
        if (rand.nextFloat() < unusualWaitRisk) {
            wait = rand.nextInt(waitTimeUnusualMax - waitTimeMin) + waitTimeMin;
        } else {
            wait = rand.nextInt(waitTimeUsualMax - waitTimeMin) + waitTimeMin;
        }
        try {
            Thread.sleep(wait);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
