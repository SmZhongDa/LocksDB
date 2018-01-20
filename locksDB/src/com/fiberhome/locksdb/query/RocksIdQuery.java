package com.fiberhome.locksdb.query;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.data.DBInfo;
import com.fiberhome.locksdb.data.Rocks;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.LocksUtil;

public class RocksIdQuery extends LocksQuery implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(RocksQuery.class);
	private final LinkedBlockingQueue<byte[]> result;
	private final CountDownLatch latch;
	private final ConcurrentHashMap<String, DBInfo> dbInfos;
	private final List<String> idList;
	private int get = 0;
	private int put = 0;

	public RocksIdQuery(CountDownLatch latch, LinkedBlockingQueue<byte[]> result, String rid, List<String> idList) {
		super(rid);
		this.latch = latch;
		this.result = result;
		this.dbInfos = Rocks.DBINFOS;
		this.idList = idList;
	}

	private void query() {
		for (String id : idList) {
			String date = LocksUtil.getPfixDate(id);
			long _id = Long.parseLong(id);
			get++;
			try {
				DBInfo info = dbInfos.get(date);
				byte[] res = info.db.get(info.columnFamilyInfos.get(Config.LCOKSTABLE).columnFamilyHandle, LocksUtil.longToBytes(_id));
				result.put(res);
				put++;
			} catch (RocksDBException | InterruptedException e) {
				logger.error("rid " + rid + " - " + e.toString());
			}
		}

	}

	@Override
	public void run() {
		try {
			query();
		} finally {
			latch.countDown();
			isDone = true;
			logger.info("RocksQuery {} get : {} , put : {}", rid, get, put);
		}
	}

}
