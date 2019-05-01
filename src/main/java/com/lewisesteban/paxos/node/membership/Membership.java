package com.lewisesteban.paxos.node.membership;

import com.lewisesteban.paxos.node.MembershipGetter;
import com.lewisesteban.paxos.rpc.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.NodeRPCHandle;

import java.util.List;

public class Membership implements MembershipGetter, MembershipRPCHandle {

    private List<NodeRPCHandle> nodes;
    private int myNodeId;
    private int nbNodes;

    public Membership(int myNodeId, List<NodeRPCHandle> nodes) {
        this.nodes = nodes;
        this.myNodeId = myNodeId;
    }

    public void start() {
        nbNodes = nodes.size();
    }

    public void stop() {

    }

    public int getMyNodeId() {
        return myNodeId;
    }

    public List<NodeRPCHandle> getMembers() {
        return nodes;
    }

    public int getNbMembers() {
        return nbNodes;
    }
}
