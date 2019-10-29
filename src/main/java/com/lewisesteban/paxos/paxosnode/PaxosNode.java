package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.paxosnode.acceptor.Acceptor;
import com.lewisesteban.paxos.paxosnode.listener.Listener;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.paxosnode.proposer.Proposer;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.paxosnode.proposer.RunningProposalManager;
import com.lewisesteban.paxos.rpc.paxos.*;
import com.lewisesteban.paxos.storage.FileAccessorCreator;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.io.IOException;
import java.util.List;

public class PaxosNode implements RemotePaxosNode, PaxosProposer {

    private Acceptor acceptor;
    private Listener listener;
    private Proposer proposer;
    private Membership membership;
    private boolean running = false;

    public PaxosNode(int myNodeId, List<RemotePaxosNode> members, StateMachine stateMachine, StorageUnit.Creator storage, FileAccessorCreator fileAccessorCreator) throws StorageException {
        membership = new Membership(myNodeId, members);
        RunningProposalManager runningProposalManager = new RunningProposalManager();
        SnapshotManager snapshotManager = new SnapshotManager(stateMachine);
        acceptor = new Acceptor(membership, storage, fileAccessorCreator);
        listener = new Listener(membership, stateMachine, runningProposalManager, snapshotManager);
        proposer = new Proposer(membership, listener, storage.make("proposer" + membership.getMyNodeId(), null), runningProposalManager, snapshotManager);
        runningProposalManager.setup(proposer, listener);
        snapshotManager.setup(listener, acceptor);
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

    public Result propose(Command command, long inst) throws IOException {
        if (!running) {
            throw new IOException("not started");
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
