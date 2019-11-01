package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.paxosnode.listener.UnneededInstanceGossipper;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.storage.FileAccessor;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.lewisesteban.paxos.NetworkFactory.*;
import static java.lang.Thread.sleep;

public class SnapshotTest extends PaxosTestCase {

    public void testLogRemovalAfterSnapshot() throws IOException, InterruptedException {
        Callable<StateMachine> stateMachine = basicStateMachine((val) -> null);
        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, new Network(), stateMachinesSingle(stateMachine, 1));
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;
        proposer.propose(cmd1, proposer.getNewInstanceId());
        proposer.propose(cmd2, proposer.getNewInstanceId());

        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100); // wait for gossip to finish and snapshot to apply
        assertEquals(2, InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length); // should still be 2 because last command has not been validated by client
        proposer.propose(cmd3, proposer.getNewInstanceId());
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100); // wait for gossip to finish and snapshot to apply
        assertEquals(1, InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length);
    }

    public void testDownloadSnapshot() throws IOException, InterruptedException {
        AtomicBoolean snapshotLoaded = new AtomicBoolean(false);
        AtomicInteger snapshotCreated = new AtomicInteger(0);
        AtomicReference<String> stateMachineError = new AtomicReference<>(null);
        Callable<StateMachine> stateMachine = () -> new BasicStateMachine() {
            @Override
            public Serializable execute(Serializable data) {
                return data;
            }

            @Override
            public void applySnapshot(Snapshot snapshot) throws StorageException {
                super.applySnapshot(snapshot);
                if (nodeId != 2)
                    stateMachineError.set("snapshot loaded on wrong server");
                else {
                    if (snapshotLoaded.get())
                        stateMachineError.set("snapshot loaded more than once");
                    else
                        snapshotLoaded.set(true);
                }
            }

            @Override
            public void applyCurrentWaitingSnapshot() throws StorageException {
                super.applyCurrentWaitingSnapshot();
                if (nodeId == 2)
                    stateMachineError.set("waiting snapshot applied on node 2");
                else
                    snapshotCreated.incrementAndGet();
            }
        };

        // init network
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(3, network, stateMachinesSame(stateMachine, 3));
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        PaxosServer lateServer = nodes.get(2).getPaxosSrv();
        network.kill(2);

        // do three commands with server 2 down, and have the others apply a snapshot
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;
        proposer.propose(new Command("0", "client", 0), 0);
        proposer.propose(new Command("1", "client", 1), 1);
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100);
        proposer.propose(new Command("2", "client", 2), 2);
        sleep(200); // wait for snapshot to finish
        FileAccessor srv2Dir = InterruptibleVirtualFileAccessor.creator(2).create("acceptor2", null);
        assertTrue(!srv2Dir.exists() || srv2Dir.listFiles() == null || srv2Dir.listFiles().length == 0);

        network.start(2);

        // proposal that will initiate snapshot request
        Result result;
        result = lateServer.propose(new Command("3", "anotherClient", 0), lateServer.getNewInstanceId());
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());

        // proposal that will initiate catching-up of command not included in snapshot
        long newInst = lateServer.getNewInstanceId();
        assertEquals(2, newInst);
        result = lateServer.propose(new Command("3", "anotherClient", 0), newInst);
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        assertEquals(2, result.getInstanceId());
        assertEquals("2", result.getReturnData());

        // successful proposal
        newInst = lateServer.getNewInstanceId();
        assertEquals(3, newInst);
        result = lateServer.propose(new Command("3", "anotherClient", 0), newInst);
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, result.getStatus());
        assertEquals(3, result.getInstanceId());
        assertEquals("3", result.getReturnData());


        // check  files (there should be 2 after snapshot)
        FileAccessor[] files = InterruptibleVirtualFileAccessor.creator(2).create("acceptor2", null).listFiles();
        assertTrue(files.length == 2 || files.length == 3); // note: there might be an extra file left that is within the snapshot (due to the fact that the acceptor is not synchronized with the snapshotting process), but the acceptor's InstanceManager will not use the information contained in it
        files = InterruptibleVirtualFileAccessor.creator(1).create("acceptor1", null).listFiles();
        assertEquals(2, files.length);
        if (!(files[0].getName().equals("inst2") || files[1].getName().equals("inst2")))
            fail();
        if (!(files[0].getName().equals("inst3") || files[1].getName().equals("inst3")))
            fail();

        // check state machine
        if (stateMachineError.get() != null) {
            System.out.println("ERROR: " + stateMachineError.get());
            fail();
        }
        assertTrue(snapshotLoaded.get());
        assertEquals(2, snapshotCreated.get());
    }

    public void testRecoveryAfterSnapshot() throws IOException, InterruptedException {
        List<Serializable> stateMachineReceived = new ArrayList<>();
        Callable<StateMachine> stateMachine = basicStateMachine((data) -> {
            synchronized (stateMachineReceived) {
                stateMachineReceived.add(data);
                System.out.println(data);
            }
            return data;
        });
        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, network, stateMachinesSame(stateMachine, 1));
        PaxosServer server = nodes.get(0).getPaxosSrv();
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;

        // do three proposals
        server.propose(cmd1, 0);
        server.propose(cmd2, 1);
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 200);
        server.propose(cmd3, 2);
        sleep(200); // wait for snapshot to finish

        network.kill(0);
        network.start(0);

        // check recovery
        Result result;
        result = server.propose(cmd4, 1);
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        assertEquals(1, result.getInstanceId());
        PrepareAnswer prepareAnswer = server.getAcceptor().reqPrepare(2, new Proposal.ID(0, 10));
        assertTrue(prepareAnswer.isPrepareOK());
        assertEquals(prepareAnswer.getAlreadyAccepted().getCommand(), cmd3);
        assertFalse(prepareAnswer.isSnapshotRequestRequired());

        // try another proposal
        result = server.propose(cmd4, server.getNewInstanceId()); // this will cause Paxos to re-execute the instance not included in the snapshot
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        result = server.propose(cmd4, server.getNewInstanceId());
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, result.getStatus());
        assertEquals(3, result.getInstanceId());
        assertEquals(cmd4.getData(), result.getReturnData());

        // check state machine
        assertEquals(5, stateMachineReceived.size()); // command THREE was lost after failure, as it was not included in the snapshot
        assertEquals(cmd1.getData(), stateMachineReceived.get(0));
        assertEquals(cmd2.getData(), stateMachineReceived.get(1));
        assertEquals(cmd3.getData(), stateMachineReceived.get(2));
        assertEquals(cmd3.getData(), stateMachineReceived.get(3));
        assertEquals(cmd4.getData(), stateMachineReceived.get(4));
    }

    public void testSlowSnapshotting() throws IOException, InterruptedException {
        Callable<StateMachine> stateMachine = () -> new BasicStateMachine() {
            @Override
            public Serializable execute(Serializable data) {
                return data;
            }

            @Override
            public void applyCurrentWaitingSnapshot() throws StorageException {
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                super.applyCurrentWaitingSnapshot();
            }
        };

        Network network = new Network();
        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, network, stateMachinesSingle(stateMachine, 1));
        PaxosServer server = nodes.get(0).getPaxosSrv();
        SnapshotManager.SNAPSHOT_FREQUENCY = 1;
        UnneededInstanceGossipper.GOSSIP_FREQUENCY = 1;

        long startTime = System.currentTimeMillis();
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, server.propose(cmd1, server.getNewInstanceId()).getStatus());
        sleep(10);
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, server.propose(cmd2, server.getNewInstanceId()).getStatus());
        sleep(10);
        Result result = server.propose(cmd3, server.getNewInstanceId());
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, result.getStatus());
        assertEquals(2, result.getInstanceId());
        assertEquals(cmd3.getData(), result.getReturnData());

        assertTrue(System.currentTimeMillis() - startTime < 1000);
    }
}
