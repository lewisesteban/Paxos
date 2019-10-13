package com.lewisesteban.paxos.paxosnode;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class InstanceVector<T> {

    private Map<Long, T> instances = new HashMap<>();
    private Callable<T> constructor;
    private long highestInstance = 0;

    public InstanceVector(Callable<T> constructor) {
        this.constructor = constructor;
    }

    public synchronized T get(Long index) {
        if (index > highestInstance) {
            highestInstance = index;
        }
        if (instances.containsKey(index)) {
            return instances.get(index);
        } else {
            try {
                T object = constructor.call();
                instances.put(index, object);
                return object;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public synchronized void set(Long index, T value) {
        if (index > highestInstance) {
            highestInstance = index;
        }
        instances.put(index, value);
    }

    public long getHighestInstance() {
        return highestInstance;
    }
}
