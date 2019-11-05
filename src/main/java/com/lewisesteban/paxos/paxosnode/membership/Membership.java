package com.lewisesteban.paxos.paxosnode.membership;

import com.lewisesteban.paxos.paxosnode.MembershipGetter;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.util.List;

public class Membership implements MembershipGetter, MembershipRPCHandle {

    private List<RemotePaxosNode> nodes;
    private int myNodeId;
    private int nbNodes;
    private RemotePaxosNode myNode = null;
    private NodeStateSupervisor supervisor = new NodeStateSupervisor(this);

    public Membership(int myNodeId, List<RemotePaxosNode> nodes) {
        this.nodes = nodes;
        this.myNodeId = myNodeId;
    }

    public void start() {
        nbNodes = nodes.size();
        supervisor.start();
    }

    public void stop() {
        supervisor.stop();
    }

    @Override
    public int getMyNodeId() {
        return myNodeId;
    }

    @Override
    public List<RemotePaxosNode> getMembers() {
        return nodes;
    }

    @Override
    public int getNbMembers() {
        return nbNodes;
    }

    @Override
    public RemotePaxosNode getMyNode() {
        if (myNode == null) {
            myNode = nodes.get(myNodeId);
        }
        return myNode;
    }

    @Override
    public void gossipMemberList(NodeHeartbeat[] memberList) {
        supervisor.receiveMemberList(memberList);
    }
}
