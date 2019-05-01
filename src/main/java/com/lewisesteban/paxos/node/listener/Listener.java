package com.lewisesteban.paxos.node.listener;

import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.rpc.ListenerRPCHandle;

public class Listener implements ListenerRPCHandle {

    private MembershipGetter memberList;

    public Listener(MembershipGetter memberList) {
        this.memberList = memberList;
    }

}
