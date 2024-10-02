import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of a leader election algorithm.
 * This algorithm uses apache zookeeper as a coordination service,
 * and depends on the global sequence number guaranteed by the zookeeper
 * to help elect the leader during an election.
 *
 * The service that is able to created a znode with the smallest
 * sequence number among all, declares itself as the leader.
 */


/**
 * To be a Watcher of the zookeeper event,
 * the class needs to implement the Watcher interface.
 */
public class LeaderElection implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    /**
     * Zookeeper server constantly keeps track of the connected
     * clients to check if they are still alive. If the zookeeper
     * server doesn't hear from the clients in this time period,
     * it assumes that the client is dead.
     */
    private static final int SESSION_TIMEOUT = 3000;


    /**
     * The zookeeper client object.
     * Will help each node interact/connect/talk to zookeeper server
     * and listen to events from the zookeeper server as well.
     */
    private ZooKeeper zooKeeper;


    private static final String ELECTION_ZNODE = "/election";

    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();

        leaderElection.selfElectForLeader(); //each node will put try to put forward itself to be the leader.
        leaderElection.electLeader(); //identify the leader.

        /*
         Put the main thread to wait state, otherwise,
         the app will stop as soon as main() finishes
         */
        leaderElection.run();
    }

    private void selfElectForLeader() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_ZNODE + "/c_";

        /*
            Each node creates a ephemeral znode on boot.
         */
        String znodeFullPath = zooKeeper.create(znodePrefix,
                new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL); //create sequential and ephemeral znodes. Crucial for leader election and re-election to work.

        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_ZNODE + "/", ""); //just the name - sequence number.

    }

    private void electLeader() throws InterruptedException, KeeperException {
        String predecessorZnodeName = "";
        Stat predecessorStat = null;

        /*
            Keep trying until I find a prev znode to hook onto
            or I become the leader. Essential of leader re-election.
         */
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_ZNODE, false); //names (without path) of the children of ELECTION_ZNODE.

            Collections.sort(children);
            String smallestChild = children.get(0);

            /*
                The node which creates the znode with smallest sequence
                number is the leader.
             */
            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader!");
                return;
            } else {
                System.out.println("I ain't the leader." + smallestChild + " is the leader." );
                int predecessorIndex = Collections.binarySearch(children, currentZnodeName) - 1; //find current node's predecessor's index.
                predecessorZnodeName = children.get(predecessorIndex);

                /*
                    Current node will watch predecessor's znode --> core of the leader re-election algorithm.
                    It might happen that by the time this line is executed and the current node is able to
                    watch prev nodes' znode, the prev node (and its ephemeral znode) might already be dead, and
                    it might happen that there is a breakage in the chain. Therefore, we do this in a loop and keep
                    trying to find a predecessor znode to hook onto or until the current node becomes the leader itself.
                 */
                predecessorStat = zooKeeper.exists(ELECTION_ZNODE + "/" + predecessorZnodeName, this);
            }
        }

        System.out.println("Watching znode " + predecessorZnodeName);
        System.out.println();
    }

    private void run() throws InterruptedException {
        /**
         * This trick will make the main thread
         * get into a wait state while the zookeeper
         * client libraries two threads (io and event) keep
         * running and doing their job.
         */
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }

        //If here, means main thread is woken up,
        //means received a disconnect event. so
        //shutdown gracefully.
        close();
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
        System.out.println("Disconnected from zookeeper. exiting..");

    }

    private void connectToZookeeper() throws IOException {
        /**
         * On initialization, the zookeeper client created two threads, out of
         * which one is the events thread. This thread receives any event sent out
         * from the zookeeper server.
         *
         * Passing the current instance as a watcher means that when an event arrives
         * from the zookeeper server, the zookeeper client library will invoke
         * the `process()` method on this instance.
         */
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    /**
     * The process method will be called on a separate (event thread, not main) thread
     * by the zookeeper library whenever an event comes from the zookeeper server.
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            //general connection,disconnect events don't have a type.
            case None -> {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) { //connected.
                    System.out.println("Node connected to zookeeper successfully.");
                } else { //disconnected

                    //wake up the main thread.
                    synchronized (zooKeeper) {
                        System.out.println("Node disconnected from zookeeper..");
                        zooKeeper.notifyAll();
                    }
                }
            }

            case NodeDeleted -> {
                try {
                    /*
                        The znode that current node was watching got deleted.
                        Fix the chain and if required re-elect the leader.
                     */
                    electLeader();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
