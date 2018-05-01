package edu.gmu.cs475.internal;

import edu.gmu.cs475.KVStore;
import org.netcrusher.NetCrusher;

import java.rmi.RemoteException;
import java.util.LinkedList;
import java.util.function.Function;

public class TestingClient extends KVStore {

	private String toString;
	private Function<String, Void> invalidateHandler;
	private Function<String, Void> getHandler;

	public void setGetHandler(Function<String, Void> getHandler) {
		this.getHandler = getHandler;
	}

	public void setInvalidateHandler(Function<String, Void> handler) {
		this.invalidateHandler = handler;
	}

	public void setToString(String toString) {
		this.toString = toString;
	}

	boolean zkDown;
	boolean rmiDown;
	private NetCrusher proxyToZK;
	private NetCrusher proxyToSelf;

	private int debug;

	public LinkedList<String> getValueServerInvokes = new LinkedList<>();
	public LinkedList<String> setValueServerInvokes = new LinkedList<>();
	public LinkedList<String> invalidateKeyInvokes = new LinkedList<>();

	@Override
	public String getValue(String key, String fromID) throws RemoteException {
		if (rmiDown)
			try {
				this.wait();
			} catch (InterruptedException ex) {
			}
		getValueServerInvokes.add(key + "," + fromID);
		String ret = super.getValue(key, fromID);
		if(getHandler != null)
			getHandler.apply(key);
		return ret;
	}

	@Override
	public void setValue(String key, String value, String fromID) {
		if (rmiDown)
			try {
				this.wait();
			} catch (InterruptedException ex) {
			}
		setValueServerInvokes.add(key + "," + value + "," + fromID);
		super.setValue(key, value, fromID);
	}

	@Override
	public void invalidateKey(String key) throws RemoteException {
		if(rmiDown)
			try {
				this.wait();
			}catch(InterruptedException ex){}
		invalidateKeyInvokes.add(key);
		if(invalidateHandler != null)
			invalidateHandler.apply(key);
		super.invalidateKey(key);
	}

	public TestingClient(String zkConnectString, NetCrusher proxyToZK, NetCrusher proxyToSelf, int rmiBind, int rmiAdvertise, int debug) {
		super(zkConnectString, rmiBind, rmiAdvertise, debug);
		this.debug = debug;
		this.proxyToSelf = proxyToSelf;
		this.proxyToZK = proxyToZK;
	}

	public void suspendAccessToZK() {
		this.proxyToZK.freeze();
		zkDown = true;
	}

	public void resumeAccessToZK() {
		zkDown = false;
		this.proxyToZK.unfreeze();
	}

	public void suspendAccessToSelf() {
		rmiDown = true;
		this.proxyToSelf.freeze();
	}

	public void resumeAccessToSelf() {
		rmiDown = false;
		this.proxyToSelf.unfreeze();
	}

	public void _cleanup() {
		super._cleanup();
		proxyToSelf.close();
		proxyToSelf.close();
	}

	@Override
	public String toString() {
		if(toString != null)
			return toString;
		return "Client #"+debug + " @port" + getLocalPort() +", connected to ZooKeeper " + (zkDown?"N":"Y") +", listening on RMI "+ (rmiDown?"N":"Y");
	}
}
