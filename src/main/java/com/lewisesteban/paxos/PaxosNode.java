package com.lewisesteban.paxos;

import com.lewisesteban.paxos.node.acceptor.Acceptor;
import com.lewisesteban.paxos.node.listener.Listener;
import com.lewisesteban.paxos.node.membership.Membership;
import com.lewisesteban.paxos.node.proposer.Proposer;
import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.ListenerRPCHandle;
import com.lewisesteban.paxos.rpc.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.NodeRPCHandle;

import java.io.Serializable;
import java.util.List;

public class PaxosNode implements NodeRPCHandle {

    private Acceptor acceptor;
    private Listener listener;
    private Proposer proposer;
    private Membership membership;
    private boolean running = false;

    public PaxosNode(int myNodeId, List<NodeRPCHandle> members) {
        membership = new Membership(myNodeId, members);
        acceptor = new Acceptor(membership);
        listener = new Listener(membership);
        proposer = new Proposer(membership);
    }

    public void start() {
        membership.start();
        running = true;
    }

    public void stop() {
        running = false;
        membership.stop();
    }

    public boolean propose(Serializable proposalData) {
        if (!running) {
            start();
        }
        return proposer.propose(proposalData);
    }

    public int getId() {
        return membership.getMyNodeId();
    }

    public AcceptorRPCHandle getAcceptor() {
        return acceptor;
    }

    public ListenerRPCHandle getListener() {
        return listener;
    }

    public MembershipRPCHandle getMembership() {
        return membership;
    }
}
