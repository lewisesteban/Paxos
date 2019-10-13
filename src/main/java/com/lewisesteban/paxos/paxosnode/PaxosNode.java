package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.paxosnode.acceptor.Acceptor;
import com.lewisesteban.paxos.paxosnode.listener.Listener;
import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.paxosnode.proposer.Proposer;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.*;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class PaxosNode implements RemotePaxosNode, PaxosProposer {

    private Acceptor acceptor;
    private Listener listener;
    private Proposer proposer;
    private Membership membership;
    private boolean running = false;

    public PaxosNode(int myNodeId, List<RemotePaxosNode> members, StateMachine stateMachine, StorageUnit storage) {
        membership = new Membership(myNodeId, members);
        acceptor = new Acceptor(membership, storage);
        listener = new Listener(membership, stateMachine);
        proposer = new Proposer(membership, listener, storage);
    }

    public void start() {
        membership.start();
        running = true;
    }

    public void stop() {
        running = false;
        membership.stop();
    }

    public long getNewInstanceId() {
        return proposer.getNewInstanceId();
    }

    public Result propose(Serializable command, long inst) throws IOException {
        if (!running) {
            return new Result(Result.CONSENSUS_FAILED);
        } else {
            return proposer.propose(command, inst);
        }
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
