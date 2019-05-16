package com.lewisesteban.paxos.node;

import java.util.Vector;
import java.util.concurrent.Callable;

public class InstanceVector<T> {

    private Vector<T> instances = new Vector<>();
    private Callable<T> constructor;

    public InstanceVector(Callable<T> constructor) {
        this.constructor = constructor;
    }

    public T get(long index) {
        while (index >= instances.size()) {
            try {
                instances.add(constructor.call());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return instances.get((int)index);
    }

    public long getSize() {
        return instances.size();
    }

    public void set(long index, T value) {
        while (index > instances.size()) {
            try {
                instances.add(constructor.call());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (index == instances.size()) {
            instances.add(value);
        } else {
            instances.set((int)index, value);
        }
    }
}
