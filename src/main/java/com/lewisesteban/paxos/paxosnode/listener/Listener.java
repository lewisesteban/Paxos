package com.lewisesteban.paxos.paxosnode.listener;

import com.lewisesteban.paxos.Logger;
import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.ClusterHandle;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.paxosnode.proposer.RunningProposalManager;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;
import com.lewisesteban.paxos.storage.StorageException;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Listener implements ListenerRPCHandle {
    private static final int MIN_MISSING_INST_FOR_CATCHING_UP = 3;
    private static final int BULK_CATCHING_UP_MAX_SIZE = 100;

    private long snapshotLastInstanceId = -1;
    private Map<Long, ExecutedCommand> executedCommands = new HashMap<>();
    private long lastInstanceId = -1;
    private ClusterHandle memberList;
    private StateMachine stateMachine;
    private RunningProposalManager runningProposalManager;
    private SnapshotManager snapshotManager;
    private CatchingUpManager catchingUpManager = null;
    private Long currentBulkCatchUp = null;

    public Listener(ClusterHandle memberList, StateMachine stateMachine,
                    RunningProposalManager runningProposalManager, SnapshotManager snapshotManager) {
        this.memberList = memberList;
        this.stateMachine = stateMachine;
        this.runningProposalManager = runningProposalManager;
        this.snapshotManager = snapshotManager;
    }

    public synchronized void setCatchingUpManager(CatchingUpManager catchingUpManager) {
        this.catchingUpManager = catchingUpManager;
    }

    private synchronized boolean catchUpBulk(long highestMissingInstance) {
        currentBulkCatchUp = highestMissingInstance;
        while (highestMissingInstance - lastInstanceId >= MIN_MISSING_INST_FOR_CATCHING_UP) {
            int bulkSize = BULK_CATCHING_UP_MAX_SIZE;
            if (highestMissingInstance - lastInstanceId < bulkSize)
                bulkSize = (int) (highestMissingInstance - lastInstanceId);
            long CUFirst = lastInstanceId + 1;
            long CULast = lastInstanceId + bulkSize;
            catchingUpManager.startCatchUp(CUFirst, CULast);
            for (long inst = CUFirst; inst <= CULast; inst++) {
                runningProposalManager.tryProposeNoOp(inst);
            }

            long lastInstIdBefore = lastInstanceId;
            while (runningProposalManager.contains(CULast)) {
                try {
                    wait();
                } catch (InterruptedException ignored) {
                }
            }
            if (!(lastInstanceId > lastInstIdBefore)) {
                currentBulkCatchUp = null;
                return false;
            }
        }
        currentBulkCatchUp = null;
        return true;
    }

    private synchronized boolean catchUpOneByOne(long highestMissingInstance) {
        if (highestMissingInstance <= snapshotLastInstanceId)
            return true;
        if (currentBulkCatchUp == null || highestMissingInstance > currentBulkCatchUp)
            runningProposalManager.tryProposeNoOp(highestMissingInstance);
        while (runningProposalManager.contains(highestMissingInstance)) {
            try {
                // to do: optimize? having each waiting instance check "contains" every time is not so good
                wait();
            } catch (InterruptedException ignored) {
            }
        }
        return highestMissingInstance <= snapshotLastInstanceId || executedCommands.containsKey(highestMissingInstance);
    }

    /**
     * Attempts to send a NoOp command and wait for consensus to be reached on a specified instance.
     * Returns true if some consensus has been reached on that instance.
     * Returns false if consensus cannot be reached (eg because of network failure).
     */
    public synchronized boolean waitForConsensusOn(long instance) {
        if (catchingUpManager != null && !runningProposalManager.contains(instance) && currentBulkCatchUp == null) {
            if (instance - lastInstanceId >= MIN_MISSING_INST_FOR_CATCHING_UP) {
                if (!catchUpBulk(instance))
                    return false;
            }
        }
        return catchUpOneByOne(instance);
    }

    @Override
    public synchronized boolean execute(long instanceId, Command command) throws IOException {
        if (executedCommands.containsKey(instanceId))
            return true;
        if (catchingUpManager != null)
            catchingUpManager.consensusReached(instanceId);
        if (instanceId > snapshotLastInstanceId + 1 && !executedCommands.containsKey(instanceId - 1)) {
            if (!waitForConsensusOn(instanceId - 1)) {
                throw new IOException("bad network state");
            }
        }
        if (instanceId <= snapshotLastInstanceId)
            return false;
        if (!executedCommands.containsKey(instanceId)) {
            Serializable result = null;
            if (!command.isNoOp()) {
                result = stateMachine.execute(command.getData());
            }
            if (instanceId > lastInstanceId) {
                lastInstanceId = instanceId;
            }
            snapshotManager.instanceFinished(instanceId);
            Logger.println("node " + memberList.getMyNodeId() + " execute inst=" + instanceId + " cmd=" + command + " cmdNb " + command.getClientCmdNb());
            executedCommands.put(instanceId, new ExecutedCommand(command, result));
        }
        return true;
    }

    /**
     * Returns the return value of a command that has been executed.
     * If that command hasn't been executed yet, it is executed and its return value is returned.
     * If the previous instance hasn't reached and cannot reach consensus because of some various failures,
     * IOException is thrown.
     */
    public synchronized Serializable getReturnOf(long instanceId, Command command) throws IOException, IsInSnapshotException {
        if (instanceId <= snapshotLastInstanceId)
            throw new IsInSnapshotException();
        if (!executedCommands.containsKey(instanceId)) {
            if (!execute(instanceId, command))
                throw new IsInSnapshotException();
        }
        if (command.isNoOp())
            return null;
        return executedCommands.get(instanceId).result;
    }

    /**
     * Checks if a command has been executed in a particular instance.
     * If it has, the executed command is returned.
     */
    public synchronized ExecutedCommand tryGetExecutedCommand(long instanceId) {
        if (!executedCommands.containsKey(instanceId)) {
            return null;
        }
        return executedCommands.get(instanceId);
    }

    public long getLastInstanceId() {
        return lastInstanceId;
    }

    @Override
    public StateMachine.Snapshot getSnapshot() throws StorageException {
        return snapshotManager.getSnapshot();
    }

    public long getSnapshotLastInstanceId() throws StorageException {
        return snapshotManager.getSnapshotLastInstance();
    }

    synchronized void setSnapshotUpTo(long newSnapshotLastInst, boolean replay) {
        if (snapshotLastInstanceId > newSnapshotLastInst)
            return;
        for (long i = snapshotLastInstanceId; i <= newSnapshotLastInst; ++i) {
            executedCommands.remove(i);
        }
        if (replay) {
            for (long i = newSnapshotLastInst + 1; i <= lastInstanceId; ++i) {
                stateMachine.execute(executedCommands.get(i).getCommand().getData());
            }
        }
        this.snapshotLastInstanceId = newSnapshotLastInst;
        if (snapshotLastInstanceId > lastInstanceId)
            lastInstanceId = snapshotLastInstanceId;
    }

    @Override
    public void gossipUnneededInstances(long[] unneededInstancesOfNodes) throws IOException {
        snapshotManager.receiveGossip(unneededInstancesOfNodes);
    }

    public class ExecutedCommand {

        ExecutedCommand(Command command, Serializable result) {
            this.command = command;
            this.result = result;
        }

        Command command;
        Serializable result;

        public Command getCommand() {
            return command;
        }

        public Serializable getResult() {
            return result;
        }
    }
}
