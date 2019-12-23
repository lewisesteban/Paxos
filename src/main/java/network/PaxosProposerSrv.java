package network;

import com.lewisesteban.paxos.paxosnode.Command;
import com.lewisesteban.paxos.paxosnode.proposer.Result;
import com.lewisesteban.paxos.rpc.paxos.PaxosProposer;

import java.io.IOException;

public class PaxosProposerSrv implements RemotePaxosProposer {
    private PaxosProposer proposer;

    PaxosProposerSrv(PaxosProposer proposer) {
        this.proposer = proposer;
    }

    @Override
    public long getNewInstanceId() throws IOException {
        return proposer.getNewInstanceId();
    }

    @Override
    public Result propose(Command command, long instanceId) throws IOException {
        return proposer.propose(command, instanceId);
    }

    @Override
    public void endClient(String clientId) throws IOException {
        proposer.endClient(clientId);
    }
}
