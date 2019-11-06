package com.lewisesteban.paxos.paxosnode.membership;

import com.lewisesteban.paxos.paxosnode.ClusterHandle;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.util.List;

public class Membership implements ClusterHandle, MembershipRPCHandle {

    private List<RemotePaxosNode> nodes;
    private int myNodeId;
    private int nbNodes;
    private RemotePaxosNode myNode = null;
    private NodeStateSupervisor supervisor;
    private Bully bully;
    private Integer leader = null;

    public Membership(int myNodeId, List<RemotePaxosNode> nodes) {
        this.nodes = nodes;
        this.myNodeId = myNodeId;
        this.bully = new Bully(this);
        this.supervisor = new NodeStateSupervisor(this);
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
    public Integer getLeaderNodeId() {
        return leader;
    }

    @Override
    public void setLeaderNodeId(Integer nodeId) {
        leader = nodeId;
        if (leader == null)
            bully.startElection();
    }

    @Override
    public void gossipMemberList(NodeHeartbeat[] memberList) {
        supervisor.receiveMemberList(memberList);
    }

    @Override
    public int bullyElection(int instanceNb) {
        return bully.receiveElectionMessage(instanceNb);
    }

    @Override
    public BullyVictoryResponse bullyVictory(int senderId, int instanceNb) {
        return bully.receiveVictoryMessage(senderId, instanceNb);
    }
}
