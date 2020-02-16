package network;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.acceptor.AcceptAnswer;
import com.lewisesteban.paxos.paxosnode.acceptor.PrepareAnswer;
import com.lewisesteban.paxos.paxosnode.proposer.Proposal;
import com.lewisesteban.paxos.rpc.paxos.AcceptorRPCHandle;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class BulkCatchingUpTest extends TestCase {
    private Proposal.ID defaultPropId = new Proposal.ID(0, 0);
    private Proposal defaultProp = new Proposal(new Command(null, "client", 0), defaultPropId);
    private final int exceptionPropNb = 283947;
    private Proposal.ID exceptionPropId = new Proposal.ID(0, exceptionPropNb);
    private Proposal exceptionProp = new Proposal(new Command(null, "client", 0), exceptionPropId);

    private AtomicInteger nbCallReqPrepare = new AtomicInteger(0);
    private AtomicInteger nbCallReqAccept = new AtomicInteger(0);
    private AtomicInteger nbCallBulkPrepare = new AtomicInteger(0);
    private AtomicInteger nbCallBulkAccept = new AtomicInteger(0);
    private AtomicInteger nbNullPrepare = new AtomicInteger(0);
    private AtomicInteger nbNullAccept = new AtomicInteger(0);

    private ExecutorService executorService = Executors.newCachedThreadPool();

    private TestAcceptor acceptor = new TestAcceptor();
    private BulkCatchingUpManager catchingUpManager = new BulkCatchingUpManager(acceptor);

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        nbCallReqPrepare.set(0);
        nbCallReqAccept.set(0);
        nbCallBulkPrepare.set(0);
        nbCallBulkAccept.set(0);
        nbNullPrepare.set(0);
        nbNullAccept.set(0);
    }

    private void checkCallNumbers(int reqPrep, int reqAcc, int bulkPrep, int bulkAcc) {
        assertEquals(reqPrep, nbCallReqPrepare.get());
        assertEquals(reqAcc, nbCallReqAccept.get());
        assertEquals(bulkPrep, nbCallBulkPrepare.get());
        assertEquals(bulkAcc, nbCallBulkAccept.get());
    }

    private Future<Boolean> doAsyncInst(int inst) {
        return executorService.submit(() -> {
            assertNotNull(catchingUpManager.sendRequest(inst, () -> acceptor.reqPrepare(inst, defaultPropId), defaultPropId, true));
            assertNotNull(catchingUpManager.sendRequest(inst, () -> acceptor.reqAccept(inst, defaultProp), defaultProp, false));
            return true;
        });
    }

    public void testRegularRequest() throws IOException {
        assertNotNull(catchingUpManager.sendRequest(0, () -> acceptor.reqPrepare(0, defaultPropId), defaultPropId, true));
        checkCallNumbers(1, 0, 0, 0);
        assertNotNull(catchingUpManager.sendRequest(0, () -> acceptor.reqAccept(0, defaultProp), defaultProp, false));
        checkCallNumbers(1, 1, 0, 0);
        assertFalse(catchingUpManager.isCatchingUp());
    }

    public void testBasicCatchingUp() throws ExecutionException, InterruptedException {
        catchingUpManager.startCatchUp(1, 3);
        assertTrue(catchingUpManager.isCatchingUp());
        Future prop2 = doAsyncInst(2);
        Future prop1 = doAsyncInst(1);
        Future prop3 = doAsyncInst(3);
        prop1.get();
        prop2.get();
        prop3.get();
        checkCallNumbers(0, 0, 1, 1);
        assertFalse(catchingUpManager.isCatchingUp());
        doAsyncInst(4).get();
        checkCallNumbers(1, 1, 1, 1);
        assertEquals(0, nbNullPrepare.get());
        assertEquals(0, nbNullAccept.get());
    }

    public void testOmitInstance() throws ExecutionException, InterruptedException {
        catchingUpManager.startCatchUp(1, 3);
        Future prop1 = doAsyncInst(1);
        Future prop3 = doAsyncInst(3);
        catchingUpManager.consensusReached(2);
        prop1.get();
        prop3.get();
        checkCallNumbers(0, 0, 1, 1);
        assertEquals(1, nbNullPrepare.get());
        assertEquals(1, nbNullAccept.get());

        // check next catching-up doesn't omit
        catchingUpManager.startCatchUp(1, 3);
        prop1 = doAsyncInst(1);
        Future prop2 = doAsyncInst(2);
        prop3 = doAsyncInst(3);
        prop1.get();
        prop2.get();
        prop3.get();
        checkCallNumbers(0, 0, 2, 2);
        assertEquals(1, nbNullPrepare.get());
        assertEquals(1, nbNullAccept.get());
    }

    public void testException() throws InterruptedException, ExecutionException {
        // prepare exception in inst 2
        catchingUpManager.startCatchUp(1, 2);
        Future prop1 = doAsyncInst(1);
        Future prop2 = executorService.submit(() ->
                catchingUpManager.sendRequest(2, () -> acceptor.reqPrepare(2, defaultPropId), exceptionPropId, true));
        try {
            prop1.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getMessage().endsWith("test exception prepare"));
        }
        try {
            prop2.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getMessage().endsWith("test exception prepare"));
        }
        prop1 = executorService.submit(() ->
                catchingUpManager.sendRequest(1, () -> acceptor.reqAccept(1, defaultProp), defaultProp, false));
        prop2 = executorService.submit(() ->
                catchingUpManager.sendRequest(2, () -> acceptor.reqAccept(2, defaultProp), defaultProp, false));
        assertNotNull(prop1.get());
        assertNotNull(prop2.get());
        assertFalse(catchingUpManager.isCatchingUp());

        // accept exception in inst 1
        catchingUpManager.startCatchUp(1, 2);
        assertTrue(catchingUpManager.isCatchingUp());
        prop1 = executorService.submit(() ->
                catchingUpManager.sendRequest(1, () -> acceptor.reqPrepare(1, defaultPropId), defaultPropId, true));
        prop2 = doAsyncInst(2);
        assertNotNull(prop1.get());
        prop1 = executorService.submit(() ->
                catchingUpManager.sendRequest(1, () -> acceptor.reqAccept(1, defaultProp), exceptionProp, false));
        try {
            prop1.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getMessage().endsWith("test exception accept"));
        }
        try {
            prop2.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getMessage().endsWith("test exception accept"));
        }
        assertFalse(catchingUpManager.isCatchingUp());

        // no exception
        catchingUpManager.startCatchUp(1, 2);
        assertTrue(catchingUpManager.isCatchingUp());
        prop1 = doAsyncInst(1);
        prop2 = doAsyncInst(2);
        prop1.get();
        prop2.get();
        assertFalse(catchingUpManager.isCatchingUp());

        checkCallNumbers(0, 0, 3, 3);
    }

    public void testOutOfCatchingUp() throws ExecutionException, InterruptedException {
        catchingUpManager.startCatchUp(10, 12);
        Future prop11 = doAsyncInst(11);

        Future propUnder = doAsyncInst(9);
        Future propAbove = doAsyncInst(13);
        propUnder.get();
        propAbove.get();
        checkCallNumbers(2, 2, 0, 0);

        // check accept is not included in catching-up (as we are currently stuck in prepare phase)
        Future propAccept = executorService.submit(() ->
                catchingUpManager.sendRequest(11, () -> acceptor.reqAccept(11, defaultProp), defaultProp, false));
        assertNotNull(propAccept.get());
        checkCallNumbers(2, 3, 0, 0);

        // finish prepare phase
        Future prop10Prepare = executorService.submit(() ->
                catchingUpManager.sendRequest(10, () -> acceptor.reqPrepare(10, defaultPropId), defaultPropId, true));
        assertNotNull(executorService.submit(() ->
                catchingUpManager.sendRequest(12, () -> acceptor.reqPrepare(12, defaultPropId), defaultPropId, true)).get());
        assertNotNull(prop10Prepare.get());

        // check prepare req is not included in catching-up (as we are now in accept phase)
        Future propPrepare = executorService.submit(() ->
                catchingUpManager.sendRequest(11, () -> acceptor.reqPrepare(11, defaultPropId), defaultPropId, true));
        assertNotNull(propPrepare.get());
        checkCallNumbers(3, 3, 1, 0);

        // finish accept phase
        Future prop10Accept = executorService.submit(() ->
                catchingUpManager.sendRequest(10, () -> acceptor.reqAccept(10, defaultProp), defaultProp, false));
        assertNotNull(executorService.submit(() ->
                catchingUpManager.sendRequest(12, () -> acceptor.reqAccept(12, defaultProp), defaultProp, false)).get());
        assertNotNull(prop10Accept.get());

        prop11.get();
        checkCallNumbers(3, 3, 1, 1);
    }

    public void testCantReachAcceptPhase() throws InterruptedException, ExecutionException {
        // simulates the case where one instance never reaches accept phase
        BulkCatchingUpManager.TIMEOUT = 1000;
        long start = System.currentTimeMillis();
        catchingUpManager.startCatchUp(1, 2);
        Future prop1 = doAsyncInst(1);
        Future prop2Prepare = executorService.submit(() ->
                catchingUpManager.sendRequest(2, () -> acceptor.reqPrepare(2, defaultPropId), defaultPropId, true));
        prop2Prepare.get();
        checkCallNumbers(0, 0, 1, 0);
        try {
            prop1.get();
        } catch (ExecutionException e) {
            assertTrue(e.getMessage().endsWith("Bulk catching-up failed"));
        }
        assertTrue(System.currentTimeMillis() - start >= BulkCatchingUpManager.TIMEOUT);
        checkCallNumbers(0, 0, 1, 0);
    }

    public void testDuplicateRequests() throws ExecutionException, InterruptedException {
        catchingUpManager.startCatchUp(1, 2);
        Future prop1 = doAsyncInst(1);
        Thread.sleep(200); // wait for prop1 to call the catching up manager method
        Future prop1bis = doAsyncInst(1);
        prop1bis.get();
        checkCallNumbers(1, 1, 0, 0);
        doAsyncInst(2).get();
        prop1.get();
        checkCallNumbers(1, 1, 1, 1);
    }

    public void testManyProposals() throws ExecutionException, InterruptedException {
        catchingUpManager.startCatchUp(100, 199);
        ArrayList<Integer> instancesToDo = new ArrayList<>();
        for (int i = 100; i <= 199; i++)
            instancesToDo.add(i);
        Random random = new Random();
        for (int i = 0; i < 100; i++)
            instancesToDo.add(random.nextInt(200) + 50);
        Collections.shuffle(instancesToDo);

        ArrayList<Future> props = new ArrayList<>();
        for (int inst : instancesToDo)
            props.add(doAsyncInst(inst));
        for (Future prop : props) {
            prop.get();
        }
        checkCallNumbers(100, 100, 1, 1);
    }

    class TestAcceptor implements AcceptorRPCHandle {

        @Override
        public PrepareAnswer reqPrepare(long instanceId, Proposal.ID propId) throws IOException {
            nbCallReqPrepare.incrementAndGet();
            if (propId.getNodePropNb() == exceptionPropNb)
                throw new IOException();
            return new PrepareAnswer(true, null);
        }

        @Override
        public AcceptAnswer reqAccept(long instanceId, Proposal proposal) throws IOException {
            nbCallReqAccept.incrementAndGet();
            if (proposal.getId().getNodePropNb() == exceptionPropNb)
                throw new IOException();
            return new AcceptAnswer(AcceptAnswer.ACCEPTED);
        }

        @Override
        public PrepareAnswer[] bulkPrepare(long[] instanceIds, Proposal.ID[] propIds) throws IOException {
            nbCallBulkPrepare.incrementAndGet();
            for (Proposal.ID id : propIds) {
                if (id != null && id.getNodePropNb() == exceptionPropNb)
                    throw new IOException("test exception prepare");
            }
            for (long inst : instanceIds) {
                if (inst < 0) {
                    nbNullPrepare.incrementAndGet();
                }
            }
            for (int i = 0; i < instanceIds.length; ++i) {
                if (instanceIds[i] >= 0 && propIds[i] == null)
                    throw new IOException("ERROR null prop");
            }
            PrepareAnswer[] answers = new PrepareAnswer[instanceIds.length];
            Arrays.fill(answers, new PrepareAnswer(true, null));
            return answers;
        }

        @Override
        public AcceptAnswer[] bulkAccept(long[] instanceIds, Proposal[] proposals) throws IOException {
            nbCallBulkAccept.incrementAndGet();
            for (Proposal prop : proposals) {
                if (prop != null && prop.getId().getNodePropNb() == exceptionPropNb)
                    throw new IOException("test exception accept");
            }
            for (long inst : instanceIds) {
                if (inst < 0) {
                    nbNullAccept.incrementAndGet();
                }
            }
            for (int i = 0; i < instanceIds.length; ++i) {
                if (instanceIds[i] >= 0 && proposals[i] == null)
                    throw new IOException("ERROR null prop");
            }
            AcceptAnswer[] answers = new AcceptAnswer[instanceIds.length];
            Arrays.fill(answers, new AcceptAnswer(AcceptAnswer.ACCEPTED));
            return answers;
        }

        @Override
        public long getLastInstance() {
            return 0;
        }

        @Override
        public long getLastPropNb() {
            return 0;
        }
    }
}
