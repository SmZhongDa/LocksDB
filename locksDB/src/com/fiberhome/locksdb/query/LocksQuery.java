package com.fiberhome.locksdb.query;

public class LocksQuery {

	String rid;
	boolean isDone = false;
	boolean shutdown = false;

	LocksQuery(String rid) {
		this.rid = rid;
	}

	public boolean isDone() {
		return isDone;
	}

	public void shutdown() {
		shutdown = true;
	}

}
