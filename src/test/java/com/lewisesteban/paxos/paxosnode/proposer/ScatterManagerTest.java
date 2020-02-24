package com.lewisesteban.paxos.paxosnode.proposer;

import com.lewisesteban.paxos.paxosnode.ClusterHandle;
import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.StateMachine;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.ListenerRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.MembershipRPCHandle;
import com.lewisesteban.paxos.rpc.paxos.RemotePaxosNode;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

public class ScatterManagerTest extends TestCase  {
    private List<ReceivedCommand> receivedCommands;
    private int waitMin;
    private int waitMax;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        receivedCommands = new CopyOnWriteArrayList<>();
        waitMin = 0;
        waitMax = 1;
    }

    public void testBasic() throws InterruptedException {
        ClusterHandle clusterHandle = new TestCluster(3);
        ScatterManager scatterManager = new ScatterManager(clusterHandle);
        scatterManager.sendCommand(1, 1, makeCmd("1"));
        scatterManager.sendCommand(1, 2, makeCmd("2"));
        scatterManager.sendCommand(2, 3, makeCmd("3"));
        Thread.sleep(100);
        assertTrue(cmdReceived(1, 1, "1"));
        assertTrue(cmdReceived(1, 2, "2"));
        assertTrue(cmdReceived(2, 3, "3"));
        assertEquals(3, receivedCommands.size());
    }

    public void testMaxCalls() throws InterruptedException {
        ClusterHandle clusterHandle = new TestCluster(3);
        ScatterManager scatterManager = new ScatterManager(clusterHandle);
        waitMin = 300;
        waitMax = 301;
        ScatterManager.MAX_CALLS_PER_NODE = 2;

        scatterManager.sendCommand(1, 1, makeCmd("1"));
        scatterManager.sendCommand(1, 2, makeCmd("2"));
        scatterManager.sendCommand(1, 3, makeCmd("3"));
        scatterManager.sendCommand(2, 4, makeCmd("4"));
        scatterManager.sendCommand(0, 5, makeCmd("5"));

        Thread.sleep(100);
        assertTrue(cmdReceived(1, 1, "1"));
        assertTrue(cmdReceived(1, 2, "2"));
        assertTrue(cmdReceived(2, 4, "4"));
        assertTrue(cmdReceived(0, 5, "5"));
        assertEquals(4, receivedCommands.size());

        Thread.sleep(300);
        assertTrue(cmdReceived(1, 3, "3"));
        assertEquals(5, receivedCommands.size());
    }

    public void testManyCalls() throws InterruptedException {
        ClusterHandle clusterHandle = new TestCluster(3);
        ScatterManager scatterManager = new ScatterManager(clusterHandle);
        waitMin = 0;
        waitMax = 200;
        ScatterManager.MAX_CALLS_PER_NODE = 20;
        int nbCallsPerNode = 100;
        ScatterManager.QUEUE_MAX_SIZE = nbCallsPerNode;

        long startTime = System.currentTimeMillis();
        for (int call = 0; call < nbCallsPerNode; call++) {
            for (int node = 0; node < clusterHandle.getNbMembers(); node++) {
                scatterManager.sendCommand(node, call, makeCmd(Integer.toString(call)));
            }
        }

        System.out.println("calls sent");
        assertTrue(System.currentTimeMillis() - startTime < waitMax);
        int prevNbReceived = 0;
        while (receivedCommands.size() < clusterHandle.getNbMembers() * nbCallsPerNode) {
            Thread.sleep(waitMax + 1);
            if (receivedCommands.size() == prevNbReceived)
                fail();
            prevNbReceived = receivedCommands.size();
            System.out.println("nbReceived=" + receivedCommands.size());
        }
    }

    public void testManyInstances() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            System.out.println(i);
            ClusterHandle clusterHandle = new TestCluster(3);
            ScatterManager scatterManager = new ScatterManager(clusterHandle);
            scatterManager.sendCommand(1, 1, makeCmd("1"));
            Thread.sleep(50);
            assertTrue(cmdReceived(1, 1, "1"));
            assertEquals(1, receivedCommands.size());
            receivedCommands.clear();
        }
    }

    public void testQueueLimit() throws InterruptedException {
        ClusterHandle clusterHandle = new TestCluster(3);
        ScatterManager scatterManager = new ScatterManager(clusterHandle);
        ScatterManager.QUEUE_MAX_SIZE = 2;
        ScatterManager.MAX_CALLS_PER_NODE = 2;
        waitMin = 200;
        waitMax = 201;

        scatterManager.sendCommand(1, 1, makeCmd("1"));
        scatterManager.sendCommand(1, 2, makeCmd("2"));
        Thread.sleep(20);
        // now max calls per node is reached
        scatterManager.sendCommand(1, 3, makeCmd("3"));
        scatterManager.sendCommand(1, 4, makeCmd("4"));
        Thread.sleep(20);
        // now max queue size is reached
        scatterManager.sendCommand(1, 5, makeCmd("5"));

        Thread.sleep(300);
        assertTrue(cmdReceived(1, 1, "1"));
        assertTrue(cmdReceived(1, 2, "2"));
        assertFalse(cmdReceived(1, 3, "3"));
        assertTrue(cmdReceived(1, 4, "4"));
        assertTrue(cmdReceived(1, 5, "5"));
        assertEquals(4, receivedCommands.size());
    }




    private boolean cmdReceived(int node, long inst, String data) {
        for (ReceivedCommand receivedCommand : receivedCommands) {
            if (receivedCommand.command.getData().equals(data) && receivedCommand.nodeId == node && receivedCommand.inst == inst) {
                return true;
            }
        }
        return false;
    }

    private Command makeCmd(String data) {
        return new Command(data, "", 0);
    }

    private class ReceivedCommand {
        private int nodeId;
        private long inst;
        private Command command;

        private ReceivedCommand(int nodeId, long inst, Command command) {
            this.nodeId = nodeId;
            this.inst = inst;
            this.command = command;
        }
    }

    private class TestListener implements ListenerRPCHandle {
        private int nodeId;
        private Random random = new Random();

        private TestListener(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public boolean execute(long instanceId, Command command) {
            receivedCommands.add(new ReceivedCommand(nodeId, instanceId, command));
            if (waitMax > waitMin) {
                int wait = random.nextInt(waitMax - waitMin) + waitMax;
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException ignored) {
                }
            } else {
                try {
                    Thread.sleep(waitMin);
                } catch (InterruptedException ignored) {
                }
            }
            return true;
        }

        @Override
        public StateMachine.Snapshot getSnapshot() {
            return null;
        }

        @Override
        public long getSnapshotLastInstanceId() {
            return 0;
        }

        @Override
        public void gossipUnneededInstances(long[] unneededInstancesOfNodes) {

        }
    }

    private class TestNode implements RemotePaxosNode {
        private int id;
        private ListenerRPCHandle listener;

        private TestNode(int id) {
            this.id = id;
            listener = new TestListener(id);
        }

        @Override
        public int getId() {
            return id;
        }

        @Override
        public int getFragmentId() {
            return 0;
        }

        @Override
        public AcceptorRPCHandle getAcceptor() {
            return null;
        }

        @Override
        public ListenerRPCHandle getListener() {
            return listener;
        }

        @Override
        public MembershipRPCHandle getMembership() {
            return null;
        }
    }

    private class TestCluster implements ClusterHandle {
        List<RemotePaxosNode> members;

        private TestCluster(int size) {
            this.members = new ArrayList<>();
            for (int i = 0; i < size; ++i) {
                members.add(new TestNode(i));
            }
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public int getMyNodeId() {
            return 0;
        }

        @Override
        public int getFragmentId() {
            return 0;
        }

        @Override
        public List<RemotePaxosNode> getMembers() {
            return members;
        }

        @Override
        public int getNbMembers() {
            return members.size();
        }

        @Override
        public Integer getLeaderNodeId() {
            return null;
        }

        @Override
        public void setLeaderNodeId(Integer nodeId) {

        }
    }
}
