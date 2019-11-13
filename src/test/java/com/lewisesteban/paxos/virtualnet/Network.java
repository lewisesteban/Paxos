package com.lewisesteban.paxos.virtualnet;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.Callable;

@SuppressWarnings({"BooleanMethodIsAlwaysInverted", "unused", "WeakerAccess"})
public class Network {

    private Map<Address, VirtualNetNode> nodes = new HashMap<>();
    private Set<Integer> isolatedRacks = new TreeSet<>();

    private int waitTimeMin = 0;
    private int waitTimeUsualMax = 0;
    private int waitTimeUnusualMax = 1;
    private float unusualWaitRisk = 0;

    public void setWaitTimes(int waitTimeMin, int waitTimeUsualMax, int waitTimeUnusualMax, float unusualWaitRisk) {
        this.waitTimeMin = waitTimeMin;
        this.waitTimeUsualMax = waitTimeUsualMax;
        this.waitTimeUnusualMax = waitTimeUnusualMax;
        this.unusualWaitRisk = unusualWaitRisk;
    }

    public void addNode(VirtualNetNode node) {
        this.nodes.put(node.getAddress(), node);
    }

    public void start(Address address) {
        nodes.get(address).start();
    }

    public void stop(Address address) {
        nodes.get(address).shutDown();
    }

    public void kill(Address address) {
        nodes.get(address).kill();
    }

    public void startAll() {
        for (VirtualNetNode node : nodes.values()) {
            if (!node.isRunning())
                node.start();
        }
    }

    public void stopAll() {
        for (VirtualNetNode node : nodes.values()) {
            if (node.isRunning())
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

    public <RT> RT tryNetCall(Callable<RT> callable, Address callerAddr, Address targetAddr) throws IOException {
        if (!canCommunicate(callerAddr, targetAddr))
            throw new InterruptedIOException();
        waitNetworkDelay();
        if (!canCommunicate(callerAddr, targetAddr))
            throw new InterruptedIOException();
        try {
            RT result = callable.call();
            if (!canCommunicate(callerAddr, targetAddr))
                throw new InterruptedIOException();
            waitNetworkDelay();
            if (!canCommunicate(callerAddr, targetAddr))
                throw new InterruptedIOException();
            return result;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void tryNetCall(Runnable runnable, Address callerAddr, Address targetAddr) throws IOException {
        if (!canCommunicate(callerAddr, targetAddr))
            throw new InterruptedIOException();
        waitNetworkDelay();
        if (!canCommunicate(callerAddr, targetAddr))
            throw new InterruptedIOException();
        try {
            runnable.run();
        } catch (Exception e) {
            throw new IOException();
        }
    }

    private boolean canCommunicate(Address nodeId1, Address nodeId2) {
        VirtualNetNode node1 = nodes.get(nodeId1);
        VirtualNetNode node2 = nodes.get(nodeId2);
        if (Thread.interrupted() || !node1.isRunning() || !node2.isRunning())
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

    public static class Address {
        private int clusterId;
        private int nodeIdInCluster;

        public Address(int clusterId, int nodeIdInCluster) {
            this.clusterId = clusterId;
            this.nodeIdInCluster = nodeIdInCluster;
        }

        public int getClusterId() {
            return clusterId;
        }

        public int getNodeIdInCluster() {
            return nodeIdInCluster;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Address) {
                Address other = (Address) obj;
                return this.clusterId == other.clusterId && this.nodeIdInCluster == other.nodeIdInCluster;
            }
            return super.equals(obj);
        }

        @Override
        public int hashCode() {
            return nodeIdInCluster + (clusterId >> 16);
        }
    }
}
