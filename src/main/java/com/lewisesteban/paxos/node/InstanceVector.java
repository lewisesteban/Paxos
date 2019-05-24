package com.lewisesteban.paxos.node;

import com.lewisesteban.paxos.InstId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class InstanceVector<T> {

    private Map<InstId, T> instances = new HashMap<>();
    private Callable<T> constructor;

    public InstanceVector(Callable<T> constructor) {
        this.constructor = constructor;
    }

    public T get(InstId index) {
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

    public void set(InstId index, T value) {
        instances.put(index, value);
    }
}
