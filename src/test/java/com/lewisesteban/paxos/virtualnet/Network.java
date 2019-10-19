package com.lewisesteban.paxos.virtualnet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

@SuppressWarnings({"BooleanMethodIsAlwaysInverted", "unused", "WeakerAccess"})
public class Network {

    private Map<Integer, VirtualNetNode> nodes = new HashMap<>();
    private Set<Integer> isolatedRacks = new TreeSet<>();

    private int waitTimeMin = 10;
    private int waitTimeUsualMax = 30;
    private int waitTimeUnusualMax = 100;
    private float unusualWaitRisk = 0.1f;

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

    public <RT> RT tryNetCall(Callable<RT> callable, int callerAddr, int targetAddr) throws IOException {
        if (!canCommunicate(callerAddr, targetAddr))
            throw new IOException();
        waitNetworkDelay();
        if (!canCommunicate(callerAddr, targetAddr))
            throw new IOException();
        try {
            RT result = callable.call();
            if (!canCommunicate(callerAddr, targetAddr))
                throw new IOException();
            waitNetworkDelay();
            if (!canCommunicate(callerAddr, targetAddr))
                throw new IOException();
            return result;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void tryNetCall(Runnable runnable, int callerAddr, int targetAddr) throws IOException {
        if (!canCommunicate(callerAddr, targetAddr))
            throw new IOException();
        waitNetworkDelay();
        if (!canCommunicate(callerAddr, targetAddr))
            throw new IOException();
        try {
            runnable.run();
        } catch (Exception e) {
            throw new IOException();
        }
    }

    private boolean canCommunicate(int nodeId1, int nodeId2) {
        VirtualNetNode node1 = nodes.get(nodeId1);
        VirtualNetNode node2 = nodes.get(nodeId2);
        if (!node1.isRunning() || !node2.isRunning())
            return false;
        int rack1 = node1.getRack();
        int rack2 = node2.getRack();
        return (rack1 == rack2) || (isRackConnected(rack1) && isRackConnected(rack2));
    }

    private void waitNetworkDelay() {
        int wait;
        Random rand = new Random();
        if (rand.nextFloat() < unusualWaitRisk) {
            wait = rand.nextInt(waitTimeUnusualMax - waitTimeMin) + waitTimeMin;
        } else {
            if (waitTimeUsualMax > waitTimeMin) {
                wait = rand.nextInt(waitTimeUsualMax - waitTimeMin) + waitTimeMin;
            } else {
                wait = waitTimeMin;
            }
        }
        try {
            Thread.sleep(wait);
        } catch (InterruptedException ignored) { }
    }
}
