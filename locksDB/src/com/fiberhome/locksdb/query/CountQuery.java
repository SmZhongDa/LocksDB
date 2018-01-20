package com.fiberhome.locksdb.query;

import io.netty.channel.ChannelHandlerContext;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.client.LocksMsg;
import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.index.LuceneReader;
import com.fiberhome.locksdb.index.ReaderHandler;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.LocksUtil;

public class CountQuery extends LocksQuery implements Runnable {

	private Logger logger = LoggerFactory.getLogger(CountQuery.class);
	private final List<String> tableList;
	private final ChannelHandlerContext ctx;
	public final CountDownLatch latch;
	private final String partitions;

	public CountQuery(List<String> tableList, String partitions, ChannelHandlerContext ctx, CountDownLatch latch, String rid) {
		super(rid);
		this.tableList = tableList;
		this.partitions = partitions;
		this.ctx = ctx;
		this.latch = latch;
	}

	@Override
	public void run() {
		try {
			query();
		} finally {
			latch.countDown();
			isDone = true;
			logger.debug("CountQuery {} is done", rid);
		}
	}

	private void query() {
		LinkedList<String> partitionList = LocksUtil.getPartitions(this.partitions);
		for (String string : partitionList) {
			for (String _s : tableList) {
				String table = _s + Config.SEPARATOR + string;
				if (Lucene.INDEXWRITERMAP.containsKey(table)) {
					if (!shutdown) {
						LuceneReader luceneReader = ReaderHandler.getReader(table);
						StringBuilder sb = new StringBuilder();
						String num = LocksUtil.stringAppend(sb, System.currentTimeMillis(), "\t3\t", rid, "\t", table, "\t", luceneReader.reader.numDocs());
						byte[] bytes = num.getBytes(Config.DEFAULTCHARSET);
						ctx.writeAndFlush(new LocksMsg(bytes.length, bytes));
						luceneReader.release();
					} else
						return;
				}
			}
		}
	}
}
