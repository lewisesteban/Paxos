package com.lewisesteban.paxos.storage;

import java.util.Map;
import java.util.TreeMap;

public class InterruptibleAccessorContainer {
    static private Map<Integer, InterruptibleStorage> map = new TreeMap<>();

    public static void add(int nodeId, InterruptibleStorage object) {
        map.put(nodeId, object);
    }

    public static void interrupt(int nodeId) {
        map.get(nodeId).interrupt();
        map.remove(nodeId);
    }

    public static void clear() {
        for (InterruptibleStorage object : map.values()) {
            object.interrupt();
        }
        map.clear();
    }
}
