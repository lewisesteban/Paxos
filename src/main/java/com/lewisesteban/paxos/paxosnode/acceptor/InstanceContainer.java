package com.lewisesteban.paxos.paxosnode.acceptor;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

public class InstanceContainer<T extends Serializable> {

    private Map<Long, T> instances = new TreeMap<>();
    private Callable<T> constructor;
    private long earliestInstanceId = 0;
    private long highestInstance = 0;

    InstanceContainer(Callable<T> constructor, Map<Long, T> source) {
        this.constructor = constructor;
        if (source != null)
            instances = source;
    }

    public synchronized T get(Long index) {
        if (index > highestInstance) {
            highestInstance = index;
        }
        if (instances.containsKey(index)) {
            return instances.get(index);
        } else {
            if (index >= earliestInstanceId) {
                try {
                    T object = constructor.call();
                    instances.put(index, object);
                    return object;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    public synchronized void set(Long index, T value) {
        if (index < earliestInstanceId)
            return;
        if (index > highestInstance) {
            highestInstance = index;
        }
        instances.put(index, value);
    }

    long getHighestInstance() {
        if (instances.isEmpty())
            return -1;
        return highestInstance;
    }

    synchronized void truncateBefore(long earliestInstanceToKeep) {
        instances.keySet().removeIf((key) -> key < earliestInstanceToKeep);
        this.earliestInstanceId = earliestInstanceToKeep;
        if (earliestInstanceToKeep > highestInstance)
            highestInstance = earliestInstanceToKeep;
    }
}
