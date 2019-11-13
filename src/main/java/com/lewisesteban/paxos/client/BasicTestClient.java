package com.lewisesteban.paxos.client;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;
import java.io.Serializable;

/**
 * Does not handle fragmentation and dedicated proposer redirection
 */
public class BasicTestClient {

    private PaxosProposer paxosNode;
    private Command.Factory commandFactory;
    private ClientCommandSender sender;

    public BasicTestClient(PaxosProposer paxosNode, String clientId) {
        this.paxosNode = paxosNode;
        this.commandFactory = new Command.Factory(clientId);
        this.sender = new ClientCommandSender(null);
    }

    /**
     * Tries to send a command. Will throw only if the network state is such that consensus cannot be reached.
     * In that case, a CommandException is thrown, containing the instance that may have been initiated.
     */
    public Serializable tryDoCommand(Serializable commandData) throws IOException {
        Command command = commandFactory.make(commandData);
        try {
            return sender.doCommand(paxosNode, command);
        } catch (ClientCommandSender.DedicatedProposerRedirection | ClientCommandSender.CommandFailedException e) {
            throw new IOException(e);
        }
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
            } catch (ClientCommandSender.CommandFailedException e) { // change error handling in the future
                if (e.getInstanceThatMayHaveBeenInitiated() != null)
                    instance = e.getInstanceThatMayHaveBeenInitiated();
            } catch (ClientCommandSender.DedicatedProposerRedirection e) {
                // not handled
                e.printStackTrace();
            }
        }
    }
}