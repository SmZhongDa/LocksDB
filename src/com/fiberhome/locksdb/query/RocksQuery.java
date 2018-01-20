package com.fiberhome.locksdb.query;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.data.DBInfo;
import com.fiberhome.locksdb.data.Rocks;
import com.fiberhome.locksdb.util.Config;

public class RocksQuery extends LocksQuery implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(RocksQuery.class);
	private final LinkedBlockingQueue<Pair<String, byte[]>> keyQueue;
	private final LinkedBlockingQueue<byte[]> result;
	private final LuceneQuery luceneQuery;
	private final CountDownLatch latch;
	private final ConcurrentHashMap<String, DBInfo> dbInfos;
	private int get = 0;
	private int put = 0;

	public RocksQuery(CountDownLatch latch, LinkedBlockingQueue<Pair<String, byte[]>> keyQueue, LinkedBlockingQueue<byte[]> result, LuceneQuery luceneQuery, String rid) {
		super(rid);
		this.latch = latch;
		this.keyQueue = keyQueue;
		this.result = result;
		this.luceneQuery = luceneQuery;
		this.dbInfos = Rocks.DBINFOS;
	}

	private void query() {
		Pair<String, byte[]> pair;
		while ((pair = keyQueue.poll()) != null) {
			get++;
			try {
				DBInfo info = dbInfos.get(pair.key);
				byte[] res = info.db.get(info.columnFamilyInfos.get(Config.LCOKSTABLE).columnFamilyHandle, pair.value);
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
			while (!luceneQuery.isDone()) {
				query();
			}
			if (!keyQueue.isEmpty())
				query();
		} finally {
			latch.countDown();
			isDone = true;
			logger.info("RocksQuery {} get : {} , put : {}", rid, get, put);
		}
	}

}
