package network;

import com.lewisesteban.paxos.paxosnode.listener.CatchingUpManager;

import java.util.List;

public class MultiClientCatchingUpManager implements CatchingUpManager {
    private List<ClientCUMGetter> clients;

    public MultiClientCatchingUpManager(List<ClientCUMGetter> clients) {
        this.clients = clients;
    }

    @Override
    public boolean isCatchingUp() {
        for (ClientCUMGetter clientCUMGetter : clients) {
            CatchingUpManager catchingUpManager = clientCUMGetter.get();
            if (catchingUpManager != null)
                return catchingUpManager.isCatchingUp();
        }
        return false;
    }

    @Override
    public void startCatchUp(long firstInstInclusive, long lastInstInclusive) {
        for (ClientCUMGetter client : clients) {
            CatchingUpManager catchingUpManager = client.get();
            if (catchingUpManager != null)
                catchingUpManager.startCatchUp(firstInstInclusive, lastInstInclusive);
        }
    }

    @Override
    public void consensusReached(long inst) {
        for (ClientCUMGetter client : clients) {
            CatchingUpManager catchingUpManager = client.get();
            if (catchingUpManager != null)
                catchingUpManager.consensusReached(inst);
        }
    }

    public interface ClientCUMGetter {
        CatchingUpManager get();
    }
}
