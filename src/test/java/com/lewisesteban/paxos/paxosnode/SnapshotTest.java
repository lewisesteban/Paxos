package com.lewisesteban.paxos.paxosnode;

import com.lewisesteban.paxos.NetworkFactory;
import com.lewisesteban.paxos.PaxosTestCase;
import com.lewisesteban.paxos.client.BasicPaxosClient;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.listener.SnapshotManager;
import com.lewisesteban.paxos.paxosnode.listener.UnneededInstanceGossipper;
import com.lewisesteban.paxos.paxosnode.membership.Membership;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;
import com.lewisesteban.paxos.storage.FileAccessor;
import com.lewisesteban.paxos.storage.SafeSingleFileStorage;
import com.lewisesteban.paxos.storage.StorageException;
import com.lewisesteban.paxos.storage.StorageUnit;
import com.lewisesteban.paxos.storage.virtual.InterruptibleVirtualFileAccessor;
import com.lewisesteban.paxos.virtualnet.Network;
import com.lewisesteban.paxos.virtualnet.paxosnet.PaxosNetworkNode;
import com.lewisesteban.paxos.virtualnet.server.PaxosServer;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.lewisesteban.paxos.NetworkFactory.*;
import static java.lang.Thread.sleep;

public class SnapshotTest extends PaxosTestCase {
    // NOTE: Let I be the current instance. A waiting snapshot for an instance X will be applied only if:
    // (I + 1) % SNAPSHOT_FREQUENCY == 0 && globalUnneededInstance >= X
    // globalUnneededInstance will be equal to X only after X has been gossipped, which happens when instance X+1 ends

    @Override
    protected void setUp() {
        Membership.LEADER_ELECTION = false;
    }

    public void testLogRemovalAfterSnapshot() throws IOException, InterruptedException {
        Callable<StateMachine> stateMachine = basicStateMachine((val) -> null);
        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, new Network(), stateMachinesSingle(stateMachine, 1));
        PaxosProposer proposer = nodes.get(0).getPaxosSrv();
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;
        proposer.propose(cmd1, proposer.getNewInstanceId());
        proposer.propose(cmd2, proposer.getNewInstanceId());
        assertEquals(2, InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length); // should still be 2 because last command has not been validated by client

        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100); // wait til I can gossip again
        proposer.propose(cmd3, proposer.getNewInstanceId()); // gossip unneeded inst 2
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100); // wait til I can gossip again
        proposer.propose(cmd4, proposer.getNewInstanceId()); // trigger snapshot
        sleep(200); // wait for snapshot to finish
        assertEquals(2, InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length);
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

        // do the first two commands, then kill 2
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;
        proposer.propose(new Command("0", "client", 0), 0);
        proposer.propose(new Command("1", "client", 1), 1);
        network.kill(2);
        assertEquals(2, InterruptibleVirtualFileAccessor.creator(2).create("acceptor2", null).listFiles().length);

        // make the others do a snapshot
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100);
        proposer.propose(new Command("2", "client", 2), 2);
        nodes.get(0).getPaxosSrv().getListener().gossipUnneededInstances(new long[] { 2, 2, 2 });
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100);
        proposer.propose(new Command("3", "client", 3), 3); // trigger snapshot
        assertEquals(2, InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length);

        // proposal that will initiate snapshot downloading
        network.start(2);
        assertEquals(2, InterruptibleVirtualFileAccessor.creator(2).create("acceptor2", null).listFiles().length);
        Result result;
        result = lateServer.propose(new Command("4", "anotherClient", 0), lateServer.getNewInstanceId());
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        FileAccessor node2Dir = InterruptibleVirtualFileAccessor.creator(2).create("acceptor2", null);
        assertTrue(node2Dir == null || node2Dir.listFiles() == null || node2Dir.listFiles().length == 0);

        // proposal that will initiate catching-up of the third command
        long newInst = lateServer.getNewInstanceId();
        assertEquals(2, newInst);
        result = lateServer.propose(new Command("4", "anotherClient", 0), newInst);
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        assertEquals(2, result.getInstanceId());
        assertEquals("2", result.getReturnData());

        // proposal that will initiate catching-up of the fourth command
        newInst = lateServer.getNewInstanceId();
        assertEquals(3, newInst);
        result = lateServer.propose(new Command("4", "anotherClient", 0), newInst);
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        assertEquals(3, result.getInstanceId());
        assertEquals("3", result.getReturnData());


        // successful proposal
        newInst = lateServer.getNewInstanceId();
        assertEquals(4, newInst);
        result = lateServer.propose(new Command("4", "anotherClient", 0), newInst);
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, result.getStatus());
        assertEquals(4, result.getInstanceId());
        assertEquals("4", result.getReturnData());


        // check  files (there should be 3 after snapshot)
        FileAccessor[] files = InterruptibleVirtualFileAccessor.creator(2).create("acceptor2", null).listFiles();
        assertTrue(files.length == 3 || files.length == 4); // note: there might be an extra file left that is within the snapshot (due to the fact that the acceptor is not synchronized with the snapshotting process), but the acceptor's InstanceManager will not use the information contained in it
        files = InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles();
        assertEquals(3, files.length);
        if (!(files[0].getName().equals("inst2") || files[1].getName().equals("inst2") || files[2].getName().equals("inst2")))
            fail();
        if (!(files[0].getName().equals("inst3") || files[1].getName().equals("inst3") || files[2].getName().equals("inst3")))
            fail();
        if (!(files[0].getName().equals("inst4") || files[1].getName().equals("inst4") || files[2].getName().equals("inst4")))
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

        // do four proposals
        server.propose(cmd1, 0);
        server.propose(cmd2, 1);
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 200); // wait til I can gossip
        server.propose(cmd3, 2);
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 200); // wait for gossip to finish
        server.propose(cmd4, 3);
        sleep(200); // wait for snapshot to finish

        network.kill(0);
        network.start(0);

        // check recovery
        Result result;
        result = server.propose(cmd5, 1);
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        assertEquals(1, result.getInstanceId());
        PrepareAnswer prepareAnswer = server.getAcceptor().reqPrepare(2, new Proposal.ID(0, 10));
        assertTrue(prepareAnswer.isPrepareOK());
        assertEquals(prepareAnswer.getAlreadyAccepted().getCommand(), cmd3);
        assertFalse(prepareAnswer.isSnapshotRequestRequired());

        // try another proposal
        result = server.propose(cmd5, server.getNewInstanceId()); // this will cause Paxos to re-execute inst3
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        result = server.propose(cmd5, server.getNewInstanceId()); // this will cause Paxos to re-execute inst4
        assertEquals(Result.CONSENSUS_ON_ANOTHER_CMD, result.getStatus());
        result = server.propose(cmd5, server.getNewInstanceId());
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, result.getStatus());
        assertEquals(4, result.getInstanceId());
        assertEquals(cmd5.getData(), result.getReturnData());

        // check state machine
        assertEquals(7, stateMachineReceived.size()); // commands THREE and FOUR were lost after failure, as they were not included in the snapshot, so they were executed twice
        assertEquals(cmd1.getData(), stateMachineReceived.get(0));
        assertEquals(cmd2.getData(), stateMachineReceived.get(1));
        assertEquals(cmd3.getData(), stateMachineReceived.get(2));
        assertEquals(cmd4.getData(), stateMachineReceived.get(3));
        assertEquals(cmd3.getData(), stateMachineReceived.get(4));
        assertEquals(cmd4.getData(), stateMachineReceived.get(5));
        assertEquals(cmd5.getData(), stateMachineReceived.get(6));
    }

    public void testSlowSnapshotting() throws IOException, InterruptedException {
        AtomicBoolean snapshotted = new AtomicBoolean(false);
        Callable<StateMachine> stateMachine = () -> new BasicStateMachine() {
            @Override
            public Serializable execute(Serializable data) {
                return data;
            }

            @Override
            public void applyCurrentWaitingSnapshot() throws StorageException {
                try {
                    snapshotted.set(true);
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
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;
        UnneededInstanceGossipper.GOSSIP_FREQUENCY = 1;

        long startTime = System.currentTimeMillis();
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, server.propose(cmd1, server.getNewInstanceId()).getStatus());
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, server.propose(cmd2, server.getNewInstanceId()).getStatus());
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100); // wait til I can gossip
        server.propose(cmd3, server.getNewInstanceId());
        sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100); // wait for gossip to finish
        server.propose(cmd4, server.getNewInstanceId());

        Result result = server.propose(cmd5, server.getNewInstanceId());
        assertEquals(Result.CONSENSUS_ON_THIS_CMD, result.getStatus());
        assertEquals(4, result.getInstanceId());
        assertEquals(cmd5.getData(), result.getReturnData());

        assertTrue(System.currentTimeMillis() - startTime < 1000);
        assertTrue(snapshotted.get());
    }

    public void testEndClient() throws IOException, InterruptedException {
        List<PaxosNetworkNode> nodes = initSimpleNetwork(1, new Network(), stateMachinesEmpty(1));
        PaxosServer server = nodes.get(0).getPaxosSrv();
        SnapshotManager.SNAPSHOT_FREQUENCY = 2;
        UnneededInstanceGossipper.GOSSIP_FREQUENCY = 1;

        server.propose(new Command(0, "client1", 0), server.getNewInstanceId());
        server.propose(new Command(0, "client2", 0), server.getNewInstanceId());
        Thread.sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100);
        server.propose(new Command(1, "client2", 1), server.getNewInstanceId());
        Thread.sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100);
        server.propose(new Command(2, "client2", 2), server.getNewInstanceId());
        Thread.sleep(200);
        assertEquals(4, InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length);

        // end client and make sure there is a snapshot
        server.endClient("client1");
        Thread.sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100);
        server.propose(new Command(3, "client2", 3), server.getNewInstanceId());
        Thread.sleep(UnneededInstanceGossipper.GOSSIP_FREQUENCY + 100);
        server.propose(new Command(4, "client2", 4), server.getNewInstanceId());
        Thread.sleep(200);
        assertTrue(InterruptibleVirtualFileAccessor.creator(0).create("acceptor0", null).listFiles().length <= 4);
    }

    public void testStress() throws InterruptedException {
        final int NB_NODES = 5;
        final int NB_CLIENTS = 10;
        final int TIME = 10000;
        final int KILLER_MAX_SLEEP = 200;

        SnapshotManager.SNAPSHOT_FREQUENCY = 3;
        UnneededInstanceGossipper.GOSSIP_FREQUENCY = 10;

        AtomicBoolean keepGoing = new AtomicBoolean(true);
        AtomicReference<Exception> error = new AtomicReference<>(null);

        final Network network = new Network();
        Iterable<Callable<StateMachine>> stateMachines = stateMachinesSame(SnapshotTestStateMachine.creator(NB_CLIENTS, error), NB_NODES);
        List<PaxosNetworkNode> nodes = NetworkFactory.initSimpleNetwork(NB_NODES, network, stateMachines);

        final Thread serialKiller = serialKiller(network, nodes, TIME, KILLER_MAX_SLEEP, SnapshotTestStateMachine::stop);

        Thread[] clients = new Thread[NB_CLIENTS];
        for (int clientId = 0; clientId < NB_CLIENTS; ++clientId) {
            final int thisClientsId = clientId;
            final int nodeId = new Random().nextInt(nodes.size());
            final PaxosProposer paxosServer = nodes.get(nodeId).getPaxosSrv();
            final BasicPaxosClient paxosHandle = new BasicPaxosClient(paxosServer, "client" + thisClientsId);
            clients[clientId] = new Thread(() -> {
                int cmdId = 0;
                while (keepGoing.get() && error.get() == null) {
                    TestCommand cmdData = new TestCommand(thisClientsId, cmdId);
                    paxosHandle.doCommand(cmdData);
                    System.out.println("FINISHED client " + thisClientsId + " cmd " + cmdId);
                    cmdId++;
                }
            });
        }

        serialKiller.start();
        for (Thread client : clients)
            client.start();

        serialKiller.join();
        keepGoing.set(false); // time's up
        for (PaxosNetworkNode node : nodes) {
            if (!node.isRunning())
                node.start();
        }
        for (Thread client : clients)
            client.join();

        if (error.get() != null)
            fail();
    }

    public void testStateMachine() throws IOException {
        AtomicReference<Exception> error = new AtomicReference<>(null);
        SnapshotTestStateMachine stateMachine = new SnapshotTestStateMachine(error, 3);
        stateMachine.setup(0);

        assertFalse(stateMachine.hasAppliedSnapshot());
        assertFalse(stateMachine.hasWaitingSnapshot());

        // commands that will be included in the snapshot (inst 2)
        stateMachine.execute(new TestCommand(0, 0));
        stateMachine.execute(new TestCommand(0, 1));
        stateMachine.execute(new TestCommand(1, 0));
        stateMachine.createWaitingSnapshot(2);
        assertFalse(stateMachine.hasAppliedSnapshot());
        assertTrue(stateMachine.hasWaitingSnapshot());

        // check waiting snapshot data
        int[] data = (int[]) stateMachine.getWaitingSnapshot().getData();
        assertEquals(3, data.length);
        assertEquals(1, data[0]);
        assertEquals(0, data[1]);
        assertEquals(-1, data[2]);

        // apply snapshot and execute other commands
        stateMachine.execute(new TestCommand(1, 1));
        stateMachine.applyCurrentWaitingSnapshot();
        assertFalse(stateMachine.hasWaitingSnapshot());
        assertTrue(stateMachine.hasAppliedSnapshot());

        // create another waiting snapshot (that will be lost)
        stateMachine.execute(new TestCommand(1, 2));
        stateMachine.createWaitingSnapshot(4);
        assertEquals(4, stateMachine.getWaitingSnapshotLastInstance());
        assertEquals(2, stateMachine.getAppliedSnapshotLastInstance());

        // new state machine - load snapshot
        stateMachine = new SnapshotTestStateMachine(error, 3);
        stateMachine.setup(0);
        assertFalse(stateMachine.hasWaitingSnapshot());
        assertTrue(stateMachine.hasAppliedSnapshot());
        assertEquals(2, stateMachine.getAppliedSnapshotLastInstance());
        stateMachine.execute(new TestCommand(1, 1));

        // load another snapshot
        stateMachine.createWaitingSnapshot(3);
        int[] newSnapshotData = new int[] { 10, 11, 12 };
        stateMachine.applySnapshot(new StateMachine.Snapshot(20, newSnapshotData));
        assertFalse(stateMachine.hasWaitingSnapshot());
        assertTrue(stateMachine.hasAppliedSnapshot());

        // check loaded snapshot
        stateMachine.execute(new TestCommand(2, 13));
        assertEquals(20, stateMachine.getAppliedSnapshotLastInstance());
        assertEquals(20, stateMachine.getAppliedSnapshot().getLastIncludedInstance());
        data = (int[]) stateMachine.getAppliedSnapshot().getData();
        assertEquals(3, data.length);
        assertEquals(10, data[0]);
        assertEquals(11, data[1]);
        assertEquals(12, data[2]);

        // test error detection
        assertNull(error.get());
        stateMachine.execute(new TestCommand(2, 13));
        assertNotNull(error.get());
        error.set(null);
        stateMachine.execute(new TestCommand(2, 14));
        assertNull(error.get());
        stateMachine.execute(new TestCommand(2, 16));
        assertNotNull(error.get());
    }

    static class SnapshotTestStateMachine implements StateMachine {
        int nodeId;
        StorageUnit storageUnit;
        int[] lastReceived;
        AtomicReference<Exception> error;
        Snapshot waitingSnapshot = null;
        Snapshot appliedSnapshot = null;
        boolean keepGoing = true;

        static final Map<Integer, SnapshotTestStateMachine> runningStateMachines = new TreeMap<>();

        SnapshotTestStateMachine(AtomicReference<Exception> error, int nbClients) {
            this.error = error;
            this.lastReceived = new int[nbClients];
            for (int i = 0; i < nbClients; ++i)
                lastReceived[i] = -1;
        }

        @Override
        public void setup(int nodeId) throws IOException {
            this.nodeId = nodeId;

            synchronized (runningStateMachines) {
                runningStateMachines.put(nodeId, this);
            }

            storageUnit = new SafeSingleFileStorage("stateMachine" + nodeId, null, InterruptibleVirtualFileAccessor.creator(nodeId));
            if (!storageUnit.isEmpty()) {
                long inst = Long.parseLong(storageUnit.read("inst"));
                int[] data = deserializeData(storageUnit.read("data"));
                appliedSnapshot = new Snapshot(inst, data);
                applySnapshot(appliedSnapshot);
            }
        }

        @Override
        public Serializable execute(Serializable data) {
            if (!keepGoing)
                return null;
            TestCommand testCommand = (TestCommand) data;
            if (testCommand.cmdNb != lastReceived[testCommand.clientId] + 1) {
                String errMsg = "error: stateMachine node " + nodeId + " client " + testCommand.clientId + " got cmd " + testCommand.cmdNb + " instead of " + (lastReceived[testCommand.clientId] + 1);
                System.err.println(errMsg);
                error.set(new Exception(errMsg));
            }
            lastReceived[testCommand.clientId] = testCommand.cmdNb;
            return data;
        }

        @Override
        public void createWaitingSnapshot(long idOfLastExecutedInstance) {
            waitingSnapshot = new Snapshot(idOfLastExecutedInstance, Arrays.copyOf(lastReceived, lastReceived.length));
        }

        @Override
        public Snapshot getWaitingSnapshot() {
            return waitingSnapshot;
        }

        @Override
        public Snapshot getAppliedSnapshot() {
            return appliedSnapshot;
        }

        @Override
        public long getWaitingSnapshotLastInstance() {
            if (waitingSnapshot == null)
                return -1;
            return waitingSnapshot.getLastIncludedInstance();
        }

        @Override
        public long getAppliedSnapshotLastInstance() {
            if (appliedSnapshot == null)
                return -1;
            return appliedSnapshot.getLastIncludedInstance();
        }

        @Override
        public boolean hasWaitingSnapshot() {
            return waitingSnapshot != null;
        }

        @Override
        public boolean hasAppliedSnapshot() {
            return appliedSnapshot != null;
        }

        @Override
        public void applySnapshot(Snapshot snapshot) throws StorageException {
            storageUnit.put("inst", Long.toString(snapshot.getLastIncludedInstance()));
            try {
                storageUnit.put("data", serializeData((int[]) snapshot.getData()));
            } catch (IOException e) {
                throw new StorageException(e);
            }
            storageUnit.flush();
            appliedSnapshot = snapshot;

            //printSnapshot((int[]) snapshot.getData(), "### node " + nodeId + " apply new snapshot");

            int[] snapshotData = (int[]) appliedSnapshot.getData();
            lastReceived = Arrays.copyOf(snapshotData, snapshotData.length);
            waitingSnapshot = null;
        }

        @Override
        public void applyCurrentWaitingSnapshot() throws StorageException {
            appliedSnapshot = waitingSnapshot;
            waitingSnapshot = null;

            storageUnit.put("inst", Long.toString(appliedSnapshot.getLastIncludedInstance()));
            try {
                storageUnit.put("data", serializeData((int[]) appliedSnapshot.getData()));
            } catch (IOException e) {
                throw new StorageException(e);
            }
            storageUnit.flush();

            //printSnapshot(lastReceived, "### node " + nodeId + " apply waiting snapshot");
        }

        @SuppressWarnings("unused")
        private void printSnapshot(int[] data, String msg) {
            StringBuilder stringBuilder = new StringBuilder(msg);
            stringBuilder.append(System.lineSeparator());
            for (int client = 0; client < data.length; ++client) {
                stringBuilder.append("\tclient ").append(client).append(" cmd ").append(data[client]);
                stringBuilder.append(System.lineSeparator());
            }
            System.out.print(stringBuilder.toString());
        }

        private String serializeData(int[] data) throws IOException {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
            objectOutputStream.writeObject(data);
            objectOutputStream.flush();
            return Base64.encode(outputStream.toByteArray());
        }

        private int[] deserializeData(String serialized) throws IOException {
            byte[] bytes = Base64.decode(serialized);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
            try {
                return (int[])objectInputStream.readObject();
            } catch (ClassNotFoundException e) {
                throw new StorageException(e);
            }
        }

        private void stop() {
            keepGoing = false;
        }

        @SuppressWarnings("SameParameterValue")
        static Callable<StateMachine> creator(int nbClients, AtomicReference<Exception> error) {
            return () -> new SnapshotTestStateMachine(error, nbClients);
        }

        static void stop(int nodeNb) {
            synchronized (runningStateMachines) {
                runningStateMachines.get(nodeNb).stop();
                runningStateMachines.remove(nodeNb);
            }
        }
    }
}
