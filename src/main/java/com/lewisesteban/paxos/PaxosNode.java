package com.lewisesteban.paxos;

import com.lewisesteban.paxos.node.acceptor.Acceptor;
import com.lewisesteban.paxos.node.listener.Listener;
import com.lewisesteban.paxos.node.membership.Membership;
import com.lewisesteban.paxos.node.proposer.Result;
import com.lewisesteban.paxos.node.proposer.Proposer;
import com.lewisesteban.paxos.rpc.*;

import java.io.IOException;
import java.util.List;

public class PaxosNode implements RemotePaxosNode, PaxosProposer {

    private Acceptor acceptor;
    private Listener listener;
    private Proposer proposer;
    private Membership membership;
    private boolean running = false;

    public PaxosNode(int myNodeId, List<RemotePaxosNode> members, Executor executor) {
        membership = new Membership(myNodeId, members);
        acceptor = new Acceptor(membership);
        listener = new Listener(membership, executor);
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

    public void stopNow() {
        running = false;
        membership.stopNow();
    }

    public Result proposeNew(Command command) throws IOException {
        if (!running) {
            return new Result(false);
        } else {
            return proposer.proposeNew(command);
        }
    }

    public Result propose(Command command, int inst) throws IOException {
        if (!running) {
            return new Result(false);
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
