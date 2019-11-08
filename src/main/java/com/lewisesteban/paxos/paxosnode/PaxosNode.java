package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.paxosnode.acceptor.Acceptor;
import com.lewisesteban.paxos.paxosnode.listener.Listener;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.paxosnode.listener.UnneededInstanceGossipper;
import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.paxosnode.proposer.ClientCommandContainer;
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
    private Membership paxosCluster;
    private UnneededInstanceGossipper unneededInstanceGossipper;
    private boolean running = false;

    public PaxosNode(int myNodeId, int fragmentId, List<RemotePaxosNode> members, StateMachine stateMachine, StorageUnit.Creator storage, FileAccessorCreator fileAccessorCreator) throws StorageException {
        paxosCluster = new Membership(myNodeId, fragmentId, members);
        RunningProposalManager runningProposalManager = new RunningProposalManager();
        SnapshotManager snapshotManager = new SnapshotManager(stateMachine);
        ClientCommandContainer clientCommandContainer = new ClientCommandContainer(storage, fileAccessorCreator, paxosCluster.getMyNodeId());
        unneededInstanceGossipper = new UnneededInstanceGossipper(clientCommandContainer, snapshotManager);
        acceptor = new Acceptor(paxosCluster, storage, fileAccessorCreator);
        listener = new Listener(paxosCluster, stateMachine, runningProposalManager, snapshotManager);
        proposer = new Proposer(paxosCluster, listener, storage.make("proposer" + paxosCluster.getMyNodeId(), null), runningProposalManager, snapshotManager, clientCommandContainer);
        runningProposalManager.setup(proposer, listener);
        snapshotManager.setup(listener, acceptor, unneededInstanceGossipper);
    }

    public void start() {
        paxosCluster.start();
        unneededInstanceGossipper.setup(paxosCluster);
        running = true;
    }

    public void stop() {
        running = false;
        paxosCluster.stop();
    }

    @Override
    public long getNewInstanceId() throws IOException {
        if (!running) {
            throw new IOException("not started");
        } else {
            return proposer.getNewInstanceId();
        }
    }

    @Override
    public Result propose(Command command, long inst) throws IOException {
        if (!running) {
            throw new IOException("not started");
        } else {
            return proposer.propose(command, inst);
        }
    }

    @Override
    public void endClient(String clientId) throws IOException {
        proposer.endClient(clientId);
    }

    @Override
    public int getId() {
        return paxosCluster.getMyNodeId();
    }

    @Override
    public int getFragmentId() {
        return paxosCluster.getFragmentId();
    }

    @Override
    public AcceptorRPCHandle getAcceptor() {
        return acceptor;
    }

    @Override
    public ListenerRPCHandle getListener() {
        return listener;
    }

    @Override
    public MembershipRPCHandle getMembership() {
        return paxosCluster;
    }
}
