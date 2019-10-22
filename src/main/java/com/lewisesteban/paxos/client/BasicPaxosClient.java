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

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     */
    public Serializable tryDoCommand(Serializable commandData) throws IOException {
        Command command = commandFactory.make(commandData);
        return sender.doCommand(paxosNode, command);
    }

    /**
     * Tries again until it succeeds. Can only succeed.
     * If all servers are down, will try again until there are up again.
     */
    public Serializable doCommand(Serializable commandData) {
        Command command = commandFactory.make(commandData);
        long instance = -1;
        while (true) {
            try {
                if (instance == -1) {
                    return sender.doCommand(paxosNode, command);
                } else {
                    return sender.doCommand(paxosNode, command, instance);
                }
            } catch (ClientCommandSender.CommandException e) { // change error handling in the future
                if (e.instanceThatMayHaveBeenInitiated != null)
                    instance = e.instanceThatMayHaveBeenInitiated;
            }
        }
    }
}