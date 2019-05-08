package com.lewisesteban.paxos.node.membership;

import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.rpc.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.RemotePaxosNode;

import java.util.List;

public class Membership implements MembershipGetter, MembershipRPCHandle {

    private List<RemotePaxosNode> nodes;
    private int myNodeId;
    private int nbNodes;

    public Membership(int myNodeId, List<RemotePaxosNode> nodes) {
        this.nodes = nodes;
        this.myNodeId = myNodeId;
    }

    public void start() {
        nbNodes = nodes.size();
    }

    public void stop() {

    }

    public void stopNow() {

    }

    public int getMyNodeId() {
        return myNodeId;
    }

    public List<RemotePaxosNode> getMembers() {
        return nodes;
    }

    public int getNbMembers() {
        return nbNodes;
    }
}
