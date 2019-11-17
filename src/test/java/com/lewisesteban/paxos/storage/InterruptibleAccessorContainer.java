package com.lewisesteban.paxos.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class InterruptibleAccessorContainer {
    static private final Map<Integer, InterruptibleStorage> map = new TreeMap<>();
    static private final List<Integer> interrupted = new ArrayList<>();

    public static void add(int nodeId, InterruptibleStorage object) {
        synchronized (map) {
            if (interrupted.contains(nodeId)) {
                Integer nodeIdObj = nodeId;
                interrupted.remove(nodeIdObj);
            }
            map.put(nodeId, object);
        }
    }

    public static void interrupt(int nodeId) {
        synchronized (map) {
            if (map.containsKey(nodeId)) {
                map.get(nodeId).interrupt();
                map.remove(nodeId);
                interrupted.add(nodeId);
            }
        }
    }

    public static void clear() {
        synchronized (map) {
            for (InterruptibleStorage object : map.values()) {
                object.interrupt();
            }
            map.clear();
            interrupted.clear();
        }
    }

    public static boolean isInterrupted(int nodeId) {
        synchronized (map) {
            return interrupted.contains(nodeId);
        }
    }
}
