package edu.gmu.cs475;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.concurrent.ConcurrentHashMap;

public class KVStore extends AbstractKVStore {
	ConcurrentHashMap<String,ConcurrentHashMap<Integer,PersistentNode>> memberships;
	LeaderLatch applier;
	TreeCache members;
	/*
	Do not change these constructors.
	Any code that you need to run when the client starts should go in initClient.
	 */
	public KVStore(String zkConnectString) {
		super(zkConnectString);
	}

	public KVStore(String zkConnectString, int i1, int i2, int i3) {
		super(zkConnectString, i1, i2, i3);
	}



	/**
	 * This callback is invoked once your client has started up and published an RMI endpoint.
	 * <p>
	 * In this callback, you will need to set-up your ZooKeeper connections, and then publish your
	 * RMI endpoint into ZooKeeper (publishing the hostname and port)
	 * <p>
	 * You will also need to set up any listeners to track ZooKeeper events
	 *
	 * CuratorFrameWork is a substitute for the ZooKeeper object
	 * @param localClientHostname Your client's hostname, which other clients will use to contact you
	 * @param localClientPort     Your client's port number, which other clients will use to contact you
	 */
	@Override
	public void initClient(String localClientHostname, int localClientPort) {		
		memberships = new ConcurrentHashMap<String,ConcurrentHashMap<Integer,PersistentNode>>();
		// getLocalConnectString() will return string concat of localClientHostname + localClientPort
		PersistentNode znode = new PersistentNode(zk, CreateMode.EPHEMERAL, false, 
				ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString(), new byte[0]);
		znode.start();
		// create a leader latch for electing leader
		applier = new LeaderLatch(zk, ZK_LEADER_NODE, getLocalConnectString());
		applier.addListener(new LeaderLatchListener(){
			@Override
			public void isLeader() {
				System.out.println(applier.getId() + " " + "is the new leader");				
			}

			@Override
			public void notLeader() {
				System.out.println(applier.getId() + " " + "is not a leader");
				
			}
		});
		// if the host name is empty
		if(memberships.get(localClientHostname) == null){
			memberships.put(localClientHostname, new ConcurrentHashMap<Integer,PersistentNode>());
			// set up a tree to watch this path only if it does not exist yet
			// so it only initialize the tree once
			// is it correct that we are making a treecache at specific path? Or just the general
			// ZK_MEMBERSHIP_NODE since that hold all the nodes?
			members = new TreeCache(zk,ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString());
			// adding a listener for changes in the treecache
			// temporary boiler plate
			TreeCacheListener instanceListener = new TreeCacheListener(){
				@Override
				public void childEvent(CuratorFramework zk, TreeCacheEvent event) throws Exception {
					switch(event.getType()){
						case CONNECTION_LOST:
							System.out.println("Connection lost from this instance");
						break;
						case CONNECTION_RECONNECTED:
							System.out.println("Connection has been restored from this instance");
						break;
						case CONNECTION_SUSPENDED:
							System.out.println("Connection has been suspended");
						break;
						case INITIALIZED:
							System.out.println("Connection is initialized from instance: " + getLocalConnectString());
						break;
						case NODE_ADDED:
							System.out.println("A node has been added from this instance: " + getLocalConnectString());
						break;
						case NODE_REMOVED:
							System.out.println("A node has been removed from this instance: " + getLocalConnectString());
						break;
						case NODE_UPDATED:
							System.out.println("This instance node value has been updated to: " + znode.getData());
						break;
						default:
							// nothing :(
						break;
					}
					
				}
				
			};
			members.getListenable().addListener(instanceListener);
			
		}
		memberships.get(localClientHostname).put(localClientPort, znode);
		try {
			members.start();
			applier.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Retrieve the value of a key
	 *
	 * @param key
	 * @return The value of the key or null if there is no such key
	 * @throws IOException if this client or the leader is disconnected from ZooKeeper
	 */
	@Override
	public String getValue(String key) throws IOException {
		return null;
	}

	/**
	 * Update the value of a key. After updating the value, this new value will be locally cached.
	 *
	 * @param key
	 * @param value
	 * @throws IOException if this client or the leader is disconnected from ZooKeeper
	 */
	@Override
	public void setValue(String key, String value) throws IOException {

	}

	/**
	 * Request the value of a key. The node requesting this value is expected to cache it for subsequent reads.
	 * <p>
	 * This command should ONLY be called as a request to the leader.
	 *
	 * @param key    The key requested
	 * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
	 * @return The value of the key, or null if there is no value for this key
	 * <p>
	 * DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
	 */
	@Override
	public String getValue(String key, String fromID) throws RemoteException {
		return null;
	}

	/**
	 * Request that the value of a key is updated. The node requesting this update is expected to cache it for subsequent reads.
	 * <p>
	 * This command should ONLY be called as a request to the leader.
	 * <p>
	 * This command must wait for any pending writes on the same key to be completed
	 *
	 * @param key    The key to update
	 * @param value  The new value
	 * @param fromID The ID of the client making the request (as returned by AbstractKVStore.getLocalConnectString())
	 */
	@Override
	public void setValue(String key, String value, String fromID) {

	}

	/**
	 * Instruct a node to invalidate any cache of the specified key.
	 * <p>
	 * This method is called BY the LEADER, targeting each of the clients that has cached this key.
	 *
	 * @param key key to invalidate
	 *            <p>
	 *            DOES NOT throw any exceptions (the RemoteException is thrown by RMI if the connection fails)
	 */
	@Override
	public void invalidateKey(String key) throws RemoteException {

	}

	/**
	 * Called when there is a state change in the connection
	 *
	 * @param client   the client
	 * @param newState the new state
	 */
	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState){
		
	}

	/**
	 * Release any ZooKeeper resources that you setup here
	 * (The connection to ZooKeeper itself is automatically cleaned up for you)
	 */
	@Override
	protected void _cleanup() {

	}
}

