package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;

import java.io.IOException;
import java.util.concurrent.Callable;

public class NodeConnection implements RemotePaxosNode, VirtualConnection {

    private PaxosNetworkNode targetNode;
    private Network network;
    private NodeConAcceptor acceptor;
    private NodeConListener listener;
    private NodeConMembership membership;
    private Network.Address callerAddr;

    public NodeConnection(PaxosNetworkNode targetPaxosNode, Network.Address callerAddr, final Network network) {
        this.targetNode = targetPaxosNode;
        this.callerAddr = callerAddr;
        this.network = network;
        acceptor = new NodeConAcceptor(this, targetNode.getPaxosSrv());
        listener = new NodeConListener(this, targetNode.getPaxosSrv());
        membership = new NodeConMembership(this, targetNode.getPaxosSrv());
    }

    @Override
    public <RT> RT tryNetCall(Callable<RT> callable) throws IOException {
        return network.tryNetCall(callable, callerAddr, targetNode.getAddress());
    }

    @Override
    public int getId() {
        return targetNode.getPaxosSrv().getId();
    }

    @Override
    public int getFragmentId() {
        return targetNode.getPaxosSrv().getFragmentId();
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
        return membership;
    }
}
