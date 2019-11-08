package com.lewisesteban.paxos.paxosnode.membership;

import com.lewisesteban.paxos.paxosnode.ClusterHandle;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;

import java.util.List;

public class Membership implements ClusterHandle, MembershipRPCHandle {
    public static boolean LEADER_ELECTION = true;

    private List<RemotePaxosNode> nodes;
    private int myNodeId;
    private int nbNodes;
    private RemotePaxosNode myNode = null;
    private NodeStateSupervisor supervisor;
    private Bully bully;
    private Integer leader = null;
    private boolean keepGoing = true;
    private int fragmentId;

    public Membership(int myNodeId, int fragmentId, List<RemotePaxosNode> nodes) {
        this.nodes = nodes;
        this.myNodeId = myNodeId;
        this.fragmentId = fragmentId;
        this.bully = new Bully(this);
        this.supervisor = new NodeStateSupervisor(this);
    }

    public void start() {
        nbNodes = nodes.size();
        if (LEADER_ELECTION)
            supervisor.start();
        keepGoing = true;
    }

    public void stop() {
        if (LEADER_ELECTION)
            supervisor.stop();
        keepGoing = false;
    }

    @Override
    public int getMyNodeId() {
        return myNodeId;
    }

    @Override
    public int getFragmentId() {
        return fragmentId;
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
        if (keepGoing) {
            if (leader == null) {
                bully.startElection();
            } else {
                supervisor.resetTimeout(nodeId);
            }
        }
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
