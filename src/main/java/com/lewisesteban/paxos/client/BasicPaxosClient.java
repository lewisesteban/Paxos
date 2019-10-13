package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;
import java.io.Serializable;

public class BasicPaxosClient {

    private PaxosProposer paxosNode;
    private Command.Factory commandFactory;
    private ClientCommandSender sender;

    public BasicPaxosClient(PaxosProposer paxosNode, String clientId) {
        this.paxosNode = paxosNode;
        this.commandFactory = new Command.Factory(clientId);
        this.sender = new ClientCommandSender();
    }

    public Serializable doCommand(Serializable commandData) throws IOException {
        Command command = commandFactory.make(commandData);
        return sender.doCommand(paxosNode, command);
    }
}