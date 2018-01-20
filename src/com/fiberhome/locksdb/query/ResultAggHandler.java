package com.fiberhome.locksdb.query;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fiberhome.locksdb.util.Config;

public class ResultAggHandler extends LocksQuery implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(ResultAggHandler.class);
	private RocksAggQuery query = null;
	private final LinkedBlockingQueue<byte[]> result;
	private final LinkedList<Pair<String, Integer>> index;
	private final LinkedBlockingQueue<String> response;
	private final CountDownLatch latch;
	private int get = 0;
	private int put = 0;

	public ResultAggHandler(CountDownLatch latch, RocksAggQuery query, LinkedBlockingQueue<byte[]> result, LinkedList<Pair<String, Integer>> index, LinkedBlockingQueue<String> response, String rid) {
		super(rid);
		this.latch = latch;
		this.query = query;
		this.result = result;
		this.index = index;
		this.response = response;
	}

	private void handleResult() {
		byte[] line;
		StringBuilder sb;
		while ((line = result.poll()) != null) {
			get++;
			sb = new StringBuilder();
			String string = new String(line, Config.DEFAULTCHARSET);
			String[] strs = string.split("\t", -1);
			int length = strs.length;
			for (int i = 0; i < index.size(); i++) {
				if (i != 0)
					sb.append("\t");
				if (length > index.get(i).value - 1)
					sb.append(index.get(i).key).append("\t").append(strs[index.get(i).value - 1]);
				else
					sb.append(index.get(i).key).append("\t").append("");
			}
			try {
				response.put(sb.toString());
				put++;
			} catch (InterruptedException e) {
				logger.error("rid " + rid + " - " + e.toString());
			}
		}

	}

	@Override
	public void run() {
		try {
			while (!query.isDone()) {
				handleResult();
			}
			if (!result.isEmpty())
				handleResult();
		}catch(Exception e){
			e.printStackTrace();
		}
		finally {
			latch.countDown();
			isDone = true;
			logger.info("ResultHandler {} get : {} , put : {}", rid, get, put);
		}

	}

}
