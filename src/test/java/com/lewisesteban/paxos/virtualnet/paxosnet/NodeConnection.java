package com.lewisesteban.paxos.virtualnet.paxosnet;

import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;

import java.io.IOException;

public class NodeConnection implements RemotePaxosNode, VirtualConnection {

    private PaxosNetworkNode targetNode;
    private Network network;
    private NodeConAcceptor acceptor;
    private NodeConListener listener;
    private NodeConMembership membership;
    private int callerAddr;

    public NodeConnection(PaxosNetworkNode targetPaxosNode, int callerAddr, final Network network) {
        this.targetNode = targetPaxosNode;
        this.callerAddr = callerAddr;
        this.network = network;
        acceptor = new NodeConAcceptor(this, targetNode.getPaxosSrv().getAcceptor());
        listener = new NodeConListener(this, targetNode.getPaxosSrv().getListener());
        membership = new NodeConMembership(this, targetNode.getPaxosSrv().getMembership());
    }

    public void tryNetCall() throws IOException {
        if (!network.tryCall(callerAddr, targetNode.getAddress()))
            throw new IOException();
    }

    public int getId() {
        return targetNode.getPaxosSrv().getId();
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
