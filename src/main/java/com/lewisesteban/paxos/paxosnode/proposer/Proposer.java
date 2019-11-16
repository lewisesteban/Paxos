package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.Logger;
import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.ClusterHandle;
import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.listener.IsInSnapshotException;
import com.lewisesteban.paxos.paxosnode.listener.Listener;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;

import java.io.IOException;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Proposer that works at the instance level, meaning it will only try to propose a value on the specified instance.
 */
public class Proposer implements PaxosProposer {

    private ClusterHandle memberList;
    private ProposalFactory propFac;
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Random random = new Random();
    private AtomicLong lastGeneratedInstanceId = new AtomicLong(-1);

    private Listener listener;
    private RunningProposalManager runningProposalManager;
    private SnapshotRequester snapshotRequester;
    private ClientCommandContainer clientCommandContainer;

    public Proposer(ClusterHandle memberList, Listener listener, StorageUnit storage,
                    RunningProposalManager runningProposalManager, SnapshotManager snapshotManager,
                    ClientCommandContainer clientCommandContainer) throws StorageException {
        this.memberList = memberList;
        this.propFac = new ProposalFactory(memberList.getMyNodeId(), storage);
        this.listener = listener;
        this.runningProposalManager = runningProposalManager;
        this.snapshotRequester = new SnapshotRequester(snapshotManager, memberList);
        this.clientCommandContainer = clientCommandContainer;
    }

    @Override
    public long getNewInstanceId() {
        if (listener.getLastInstanceId() > lastGeneratedInstanceId.get()) {
            lastGeneratedInstanceId.set(listener.getLastInstanceId());
        }
        return lastGeneratedInstanceId.incrementAndGet();
    }

    @Override
    public Result propose(Command command, long instanceId) throws StorageException {
        if (!command.isNoOp() && Membership.LEADER_ELECTION) {
            Integer leaderNodeId = memberList.getLeaderNodeId();
            if (leaderNodeId != null && leaderNodeId != memberList.getMyNodeId()) {
                return new Result(instanceId, leaderNodeId);
            }
        }
        clientCommandContainer.putCommand(command, instanceId);
        return propose(command, instanceId, false);
    }

    Result propose(Command command, long instanceId, boolean alreadyStartedInManager) throws StorageException {
        if (!alreadyStartedInManager) {
            try {
                runningProposalManager.startProposal(instanceId);
            } catch (RunningProposalManager.InstanceAlreadyRunningException e) {
                listener.waitForConsensusOn(instanceId);
                Listener.ExecutedCommand consensusCmd = listener.tryGetExecutedCommand(instanceId);
                if (consensusCmd == null)
                    return new Result(Result.NETWORK_ERROR, instanceId);
                if (consensusCmd.getCommand().equals(command))
                    return new Result(Result.CONSENSUS_ON_THIS_CMD, instanceId, consensusCmd.getResult());
                else
                    return new Result(Result.CONSENSUS_ON_ANOTHER_CMD, instanceId, consensusCmd.getResult());
            }
        }
        try {
            return propose(command, instanceId, 0);
        } finally {
            runningProposalManager.paxosFinished(instanceId);
        }
    }

    private Result tryAgain(Command command, long instanceId, int attempt, boolean badNetworkState) throws StorageException {
        if (!command.isNoOp() && Membership.LEADER_ELECTION) {
            Integer leaderNodeId = memberList.getLeaderNodeId();
            if (leaderNodeId != null && leaderNodeId != memberList.getMyNodeId()) {
                return new Result(instanceId, leaderNodeId);
            }
        }
        Logger.println("tryAgain node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + command +  " attempt=" + attempt + " badnetworkState=" + badNetworkState);
        if (badNetworkState) {
            if (attempt < 3) {
                return propose(command, instanceId, attempt + 1);
            } else {
                Logger.println("network_error node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + command +  " attempt=" + attempt);
                return new Result(Result.NETWORK_ERROR);
            }
        } else {
            try {
                Thread.sleep(random.nextInt(20 * (attempt + 1)));
            } catch (InterruptedException ignored) { }
            return propose(command, instanceId, attempt + 1);
        }
    }

    private Result propose(Command command, long instanceId, int attempt) throws StorageException {
        Logger.println("propose node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + command +  " attempt=" + attempt);

        Listener.ExecutedCommand thisInstanceExecutedCmd = listener.tryGetExecutedCommand(instanceId);
        if (thisInstanceExecutedCmd != null) {
            boolean sameCmd = (thisInstanceExecutedCmd.getCommand().equals(command));
            return new Result(sameCmd ? Result.CONSENSUS_ON_THIS_CMD : Result.CONSENSUS_ON_ANOTHER_CMD,
                    instanceId, thisInstanceExecutedCmd.getResult());
        }

        // prepare
        Proposal originalProposal = propFac.make(command);
        Proposal prepared;
        try {
            PrepareResult res = prepare(instanceId, originalProposal);
            if (res.isSnapshotRequestRequired()) {
                return requestSnapshot(instanceId);
            }
            prepared = res.getPreparedProposal();
        } catch (IOException e) {
            return tryAgain(command, instanceId, attempt, true);
        }
        if (prepared == null) {
            return tryAgain(command, instanceId, attempt, false);
        }
        Logger.println("prepared node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + prepared.getCommand() +  " attempt=" + attempt);

        // accept
        boolean proposalChanged = !originalProposal.getCommand().equals(prepared.getCommand());
        try {
            AcceptAnswer acceptAnswer = accept(instanceId, prepared);
            if (acceptAnswer.getAnswer() == AcceptAnswer.REFUSED) {
                return tryAgain(command, instanceId, attempt, false);
            } else if (acceptAnswer.getAnswer() == AcceptAnswer.SNAPSHOT_REQUEST_REQUIRED) {
                return requestSnapshot(instanceId);
            }
        } catch (IOException e) {
            return tryAgain(command, instanceId, attempt, true);
        }
        Logger.println("accepted node=" + memberList.getMyNodeId() + " inst=" + instanceId + " cmd=" + prepared.getCommand() +  " changed=" + proposalChanged);

        // scatter and return
        scatter(instanceId, prepared);
        java.io.Serializable returnData;
        try {
            returnData = listener.getReturnOf(instanceId, prepared.getCommand());
        } catch (IOException e) {
            return tryAgain(command, instanceId, attempt, true);
        } catch (IsInSnapshotException e) {
            return new Result(Result.CONSENSUS_ON_ANOTHER_CMD, instanceId, null);
        }
        byte resultStatus = proposalChanged ? Result.CONSENSUS_ON_ANOTHER_CMD : Result.CONSENSUS_ON_THIS_CMD;
        return new Result(resultStatus, instanceId, returnData);
    }

    private PrepareResult prepare(long instanceId, Proposal proposal) throws IOException {
        final AtomicInteger nbOk = new AtomicInteger(0);
        final AtomicInteger nbFailed = new AtomicInteger(0);
        final AtomicBoolean snapshotRequestRequired = new AtomicBoolean(false);
        final Queue<Proposal> alreadyAcceptedProps = new ConcurrentLinkedQueue<>();
        final Semaphore anyThread = new Semaphore(0);
        for (RemotePaxosNode node : memberList.getMembers()) {
            executor.submit(() -> {
                try {
                    PrepareAnswer answer = node.getAcceptor().reqPrepare(instanceId, proposal.getId());
                    if (answer.isPrepareOK()) {
                        if (answer.getAlreadyAccepted() != null) {
                            alreadyAcceptedProps.add(answer.getAlreadyAccepted());
                        }
                        nbOk.getAndIncrement();
                    } else {
                        if (answer.isSnapshotRequestRequired()) {
                            snapshotRequestRequired.set(true);
                        }
                        // Someone else is proposing or snapshot is required: abandon proposal
                        anyThread.release(memberList.getNbMembers());
                    }
                } catch (IOException ignored) {
                    nbFailed.incrementAndGet();
                } finally {
                    anyThread.release();
                }
            });
        }

        int runningThreads = memberList.getNbMembers();
        while (nbOk.get() <= memberList.getNbMembers() / 2 && runningThreads > 0) {
            try {
                anyThread.acquire();
                runningThreads--;
            } catch (InterruptedException ignored) { }
            if (snapshotRequestRequired.get()) {
                return new PrepareResult(null, true);
            }
        }

        if (nbOk.get() > memberList.getNbMembers() / 2) {
            return new PrepareResult(getNewProp(alreadyAcceptedProps, proposal), false);
        }
        if (nbFailed.get() > 0 && nbFailed.get() >= memberList.getNbMembers() / 2) {
            throw new IOException("bad network state");
        }
        return new PrepareResult(null, false);
    }

    private Proposal getNewProp(final Queue<Proposal> alreadyAcceptedProps, Proposal originalProp) {
        if (!alreadyAcceptedProps.isEmpty()) {
            Proposal highestIdProp = null;
            for (Proposal prop : alreadyAcceptedProps) {
                if (highestIdProp == null || prop.getId().isGreaterThan(highestIdProp.getId())) {
                    highestIdProp = prop;
                }
            }
            return new Proposal(highestIdProp.getCommand(), originalProp.getId());
        } else {
            return originalProp;
        }
    }

    private AcceptAnswer accept(long instanceId, Proposal proposal) throws IOException {
        final AtomicInteger nbOk = new AtomicInteger(0);
        final AtomicInteger nbFailed = new AtomicInteger(0);
        final AtomicBoolean snapshotRequestRequired = new AtomicBoolean(false);
        final Semaphore anyThread = new Semaphore(0);
        for (RemotePaxosNode node : memberList.getMembers()) {
            executor.submit(() -> {
                try {
                    AcceptAnswer answer = node.getAcceptor().reqAccept(instanceId, proposal);
                    if (answer.getAnswer() == AcceptAnswer.ACCEPTED) {
                        nbOk.incrementAndGet();
                    } else if (answer.getAnswer() == AcceptAnswer.SNAPSHOT_REQUEST_REQUIRED) {
                        snapshotRequestRequired.set(true);
                    }
                } catch (IOException ignored) {
                    nbFailed.incrementAndGet();
                } finally {
                    anyThread.release();
                }
            });
        }

        int runningThreads = memberList.getNbMembers();
        while (nbOk.get() <= memberList.getNbMembers() / 2 && runningThreads > 0) {
            try {
                anyThread.acquire();
                runningThreads--;
            } catch (InterruptedException ignored) { }
            if (snapshotRequestRequired.get()) {
                return new AcceptAnswer(AcceptAnswer.SNAPSHOT_REQUEST_REQUIRED);
            }
        }
        if (nbOk.get() > memberList.getNbMembers() / 2)
            return new AcceptAnswer(AcceptAnswer.ACCEPTED);
        if (nbFailed.get() >= memberList.getNbMembers() / 2)
            throw new IOException("bad network state");
        return new AcceptAnswer(AcceptAnswer.REFUSED);
    }

    private void scatter(long instanceId, Proposal prepared) {
        try {
            listener.execute(instanceId, prepared.getCommand());
        } catch (IOException ignored) { }
        for (RemotePaxosNode node : memberList.getMembers()) {
            if (node.getId() != memberList.getMyNodeId()) {
                executor.submit(() -> {
                    try {
                        node.getListener().execute(instanceId, prepared.getCommand());
                    } catch (IOException ignored) { }
                });
            }
        }
    }

    private Result requestSnapshot(long instanceId) {
        try {
            Logger.println("requestSnapshot node=" + memberList.getMyNodeId() + " inst=" + instanceId);
            snapshotRequester.requestSnapshot(instanceId);
            return new Result(Result.CONSENSUS_ON_ANOTHER_CMD, instanceId);
        } catch (IOException e) {
            return new Result(Result.NETWORK_ERROR, instanceId);
        }
    }

    @Override
    public void endClient(String clientId) throws IOException {
        clientCommandContainer.deleteClient(clientId);
    }

    private class PrepareResult {
        private Proposal preparedProposal;
        private boolean snapshotRequestRequired;

        PrepareResult(Proposal preparedProposal, boolean snapshotRequestRequired) {
            this.preparedProposal = preparedProposal;
            this.snapshotRequestRequired = snapshotRequestRequired;
        }

        Proposal getPreparedProposal() {
            return preparedProposal;
        }

        boolean isSnapshotRequestRequired() {
            return snapshotRequestRequired;
        }
    }
}
