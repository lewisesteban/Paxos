package com.lewisesteban.paxos.virtualnet;

import java.util.HashMap;
import java.util.Map;

public class Network {

    private Map<Integer, VirtualNode> nodes = new HashMap<>();

    public boolean tryCall(int from, int to) {
        if (nodes.get(from).isRunning() && nodes.get(to).isRunning()) {
            System.out.println("Network call from " + from + " to " + to);
            return true;
        } else {
            return false;
        }
    }

    public void addNode(VirtualNode node) {
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
        for (VirtualNode node : nodes.values()) {
            node.start();
        }
    }

    public void stopAll() {
        for (VirtualNode node : nodes.values()) {
            node.shutDown();
        }
    }

    public void killAll() {
        for (VirtualNode node : nodes.values()) {
            node.kill();
        }
    }

    public void startRack(int rack) {
        for (VirtualNode node : nodes.values()) {
            if (node.getRack() == rack) {
                node.start();
            }
        }
    }

    public void stopRack(int rack) {
        for (VirtualNode node : nodes.values()) {
            if (node.getRack() == rack) {
                node.shutDown();
            }
        }
    }

    public void killRack(int rack) {
        for (VirtualNode node : nodes.values()) {
            if (node.getRack() == rack) {
                node.kill();
            }
        }
    }
}
