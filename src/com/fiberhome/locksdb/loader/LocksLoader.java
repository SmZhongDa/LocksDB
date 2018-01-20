package com.fiberhome.locksdb.loader;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.util.ConfigLoader;
import com.fiberhome.locksdb.util.MetaDataLoader;

public class LocksLoader {

	Logger logger = LoggerFactory.getLogger(LocksLoader.class);
	LinkedBlockingQueue<String> errorData;
	boolean isDone;
	SimpleDateFormat sdf;
	HashMap<String, Integer> captureTimeMap;
	Integer interval;
	volatile long counter;

	LocksLoader(LinkedBlockingQueue<String> errorData) {
		this.errorData = errorData;
		this.sdf = new SimpleDateFormat("yyyyMMdd");
		this.sdf.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
		this.captureTimeMap = MetaDataLoader.CAPTURETIMEMAP;
		this.interval = Integer.parseInt(ConfigLoader.CONFIGMAP.get("interval"));
	}

	public boolean isDone() {
		return isDone;
	}

	public long getCounter() {
		return counter;
	}

	void putErrorData(String line) {
		try {
			errorData.put(line);
		} catch (InterruptedException e) {
			logger.error(e.toString());
		}
	}

}
