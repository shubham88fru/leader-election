import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

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

    public static void main(String[] args) throws IOException, InterruptedException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();

        /*
         Put the main thread to wait state, otherwise,
         the app will stop as soon as main() finishes
         */
        leaderElection.run();
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
        }
    }
}
