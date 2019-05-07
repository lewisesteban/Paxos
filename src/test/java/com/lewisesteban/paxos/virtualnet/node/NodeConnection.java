package com.lewisesteban.paxos.virtualnet.node;

import com.lewisesteban.paxos.rpc.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.ListenerRPCHandle;
import com.lewisesteban.paxos.rpc.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.RemotePaxosNode;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.VirtualConnection;

import java.io.IOException;

public class NodeConnection implements RemotePaxosNode, VirtualConnection {

    private PaxosNodeWrapper targetNode;
    private Network network;
    private NodeConAcceptor acceptor;
    private NodeConListener listener;
    private NodeConMembership membership;
    private int callerAddr;

    public NodeConnection(PaxosNodeWrapper targetPaxosNode, int callerAddr, final Network network) {
        this.targetNode = targetPaxosNode;
        this.callerAddr = callerAddr;
        this.network = network;
        acceptor = new NodeConAcceptor(this, targetNode.getPaxosNode().getAcceptor());
        listener = new NodeConListener(this, targetNode.getPaxosNode().getListener());
        membership = new NodeConMembership(this, targetNode.getPaxosNode().getMembership());
    }

    public void tryNetCall() throws IOException {
        if (!network.tryCall(callerAddr, targetNode.getAddress()))
            throw new IOException();
    }

    public int getId() {
        return targetNode.getPaxosNode().getId();
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
