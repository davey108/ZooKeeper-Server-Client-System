package edu.gmu.cs475;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVStore extends AbstractKVStore {
	ConcurrentHashMap<String,String> keyValueMap;
	// map key to the clients who has the key
	ConcurrentHashMap<String,ArrayList<String>> keyNodeMap;
	// map a key to a lock, assume only the leader can use this
	ConcurrentHashMap<String,ReentrantReadWriteLock> keyLockMap;
	LeaderLatch applier;
	TreeCache members;
	// debug flag. DELETE AFTER
	boolean debug = false;
	// see if this node is still connect to zk
	boolean isConnected;
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
	@SuppressWarnings("resource")
	@Override
	public void initClient(String localClientHostname, int localClientPort) {
		// getLocalConnectString() will return string concat of localClientHostname + localClientPort
		PersistentNode znode = new PersistentNode(zk, CreateMode.EPHEMERAL, false, 
				ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString(), new byte[0]);
		znode.start();
		// create a leader latch for electing leader
		applier = new LeaderLatch(zk, ZK_LEADER_NODE, getLocalConnectString());
		members = new TreeCache(zk,ZK_MEMBERSHIP_NODE);
		// initialize all structures for storing datas
		keyValueMap = new ConcurrentHashMap<String,String>();
		// the two data structure below here should only EVER be use by the leader
		keyNodeMap = new ConcurrentHashMap<String,ArrayList<String>>();
		keyLockMap = new ConcurrentHashMap<String,ReentrantReadWriteLock>();	
		try {
			members.start();
			applier.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
		applier.addListener(new LeaderLatchListener(){
			@Override
			public void isLeader() {
				if(debug){					
					System.out.println(getLocalConnectString() + " is now leader");
				}
			}
			@Override
			public void notLeader() {
				if(debug){
					System.out.println(getLocalConnectString() + " is now NOT leader");
				}
			}
		});
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
		if(debug){
			String k = "testWriteReadOneKVServer-key-1";
			System.out.println("Client Map: " + keyValueMap);
			System.out.println("Using contains method to see if key in: " + keyValueMap.contains(k));
			System.out.println("Using containsKey to see if key in: " + keyValueMap.containsKey(k));
		}
		// if node isn't connected, throw IO
		if(!isConnected){
			throw new IOException();
		}
		if(keyValueMap.containsKey(key)){
			if(debug){
				System.out.println("Contains the key!");
			}
			return keyValueMap.get(key);
		}
		// contact leader if doesn't have
		else{
			String value = null;
			try {
				value = connectToKVStore(applier.getLeader().getId()).getValue(key,this.getLocalConnectString());
				// attempts to update the cache
				if(debug){
					System.out.println("Value from leader: " + value);
				}
				if(value != null)
					keyValueMap.put(key, value);				
			}
			// not sure what to do here...
			catch (Exception e) {
				if(debug){
					System.out.println("Exception in getValue of followers");
				}
				throw new IOException();
			}
			return value;
		}
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
		if(!isConnected){
			throw new IOException();
		}
		try {
			connectToKVStore(applier.getLeader().getId()).setValue(key, value, this.getLocalConnectString());
		} catch (NotBoundException e) {
			throw new IOException();
		} catch (Exception e) {
			throw new IOException();
		}
		this.keyValueMap.put(key, value);
	
		
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
		String value = null;
		if(debug){
			System.out.println("The map: " + keyValueMap);
			System.out.println("Result for key: " + key + " in table: " + keyValueMap.contains(key));
		}
		if(!this.keyLockMap.containsKey(key)){
			keyLockMap.put(key, new ReentrantReadWriteLock(true));
		}
		keyLockMap.get(key).readLock().lock();
		try{
			if(this.keyValueMap.containsKey(key)){
				value = this.keyValueMap.get(key);
				// update cache for such client that it has the value of this key
				if(this.keyNodeMap.get(key) == null){
					this.keyNodeMap.put(key, new ArrayList<String>());
				}
				this.keyNodeMap.get(key).add(fromID);
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			keyLockMap.get(key).readLock().unlock();
		}
		if(debug){
			System.out.println("value from leader: " + value);
		}
		return value;
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
	public void setValue(String key, String value, String fromID) throws IOException{		
		if(!isConnected){
			throw new IOException();
		}
		// if the key doesn't exist, make it exist and install a lock for it
		if(!keyLockMap.containsKey(key)){
			keyLockMap.put(key, new ReentrantReadWriteLock(true));
		}
		// now put a lock on that key
		keyLockMap.get(key).writeLock().lock();
		try{
			Map<String,ChildData> followers = this.members.getCurrentChildren(ZK_MEMBERSHIP_NODE);
			// check which node is connected to key that is in still alive followers
			// if the list of client with that key cached exist and isn't empty
			if(keyNodeMap.get(key) != null && keyNodeMap.get(key).size() != 0){
				ArrayList<String> clientsKey = keyNodeMap.get(key);
				for(String client : clientsKey){
					// if the followers contains the client cached, then access the client and remove cache
					if(followers.containsKey(client)){
						connectToKVStore(client).invalidateKey(key);
					}
				}
			}
			// clear the leader's cache of client who has this key
			if(debug){
				System.out.println("Leader is about to set the key: " + key + " to value: " + value);
			}
			keyValueMap.put(key, value);
			if(debug){
				System.out.println(keyValueMap.get(key));
			}
			// reenter the client cached
			keyNodeMap.put(key, new ArrayList<String>());
			keyNodeMap.get(key).add(fromID);			
			
		} catch (RemoteException | NotBoundException e) {
			if(debug){
				System.out.println("Exception in set value");
			}
			e.printStackTrace();
		}
		finally{
			keyLockMap.get(key).writeLock().unlock();
		}
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
		// assume that the node already has the key so we just remove it
		this.keyValueMap.remove(key);
	}

	/**
	 * Called when there is a state change in the connection
	 *
	 * @param client   the client
	 * @param newState the new state
	 */
	@Override
	public void stateChanged(CuratorFramework client, ConnectionState newState){
		// see if the node is still connected to zk
		isConnected = newState.isConnected();
		switch(newState)
		{
		// does nothing in connected
		case CONNECTED:
		{
			System.out.println("Node connected");
			break;
		}
		case RECONNECTED:
		{
			System.out.println("Node reconnected");
			// if this node came back online and it isn't the leader, then flush its cache
			try {
				if(!this.applier.getLeader().getId().equals(applier.getId())){
					keyValueMap = new ConcurrentHashMap<String,String>();
					// the two data structure below here should only EVER be use by the leader
					keyNodeMap = new ConcurrentHashMap<String,ArrayList<String>>();
					keyLockMap = new ConcurrentHashMap<String,ReentrantReadWriteLock>();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}			
			break;
		}
		case SUSPENDED:
		{
			System.out.println("Node suspended");	
		}
		case LOST:
		{
			System.out.println("Node Lost");
			// erase all states since node is lost
			keyValueMap = null;
			keyNodeMap = null;
			keyLockMap = null;
			//client.close();
			break;
		}
		case READ_ONLY:
			System.out.println("Read-only...weird");
			break;
		}
	}

	/**
	 * Release any ZooKeeper resources that you setup here
	 * (The connection to ZooKeeper itself is automatically cleaned up for you)
	 */
	@Override
	protected void _cleanup() {
		// close leader latch
		try {
			applier.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// close treecache
		members.close();
		// deleting the node from zookeeper
		/*try {
			// check first if node still exist...not sure if this is right
			if(zk.checkExists().forPath(ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString()) != null){
				zk.delete().guaranteed().deletingChildrenIfNeeded().forPath(ZK_MEMBERSHIP_NODE + "/" + getLocalConnectString());
			
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}
}

