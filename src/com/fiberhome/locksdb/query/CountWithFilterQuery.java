package com.fiberhome.locksdb.query;

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.client.LocksMsg;
import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.index.LuceneReader;
import com.fiberhome.locksdb.index.ReaderHandler;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.LocksUtil;

public class CountWithFilterQuery extends LocksQuery implements Runnable {

	private Logger logger = LoggerFactory.getLogger(CountWithFilterQuery.class);
	private final Query query;
	private final String partitions;
	private final String table;
	private final ChannelHandlerContext ctx;
	public final CountDownLatch latch;
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

	public CountWithFilterQuery(String query, String partitions, String table, ChannelHandlerContext ctx, CountDownLatch latch, String rid) throws ParseException {
		super(rid);
		QueryParser qp = new QueryParser("", Config.defaultAnalyzer());
		qp.setLowercaseExpandedTerms(false);
		qp.setAllowLeadingWildcard(true);
		this.query = qp.parse(query);
		this.partitions = partitions;
		this.table = table;
		this.ctx = ctx;
		this.latch = latch;
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
	}

	@Override
	public void run() {
		try {
			query();
		} finally {
			latch.countDown();
			isDone = true;
			logger.debug("CountWithFilterQuery {} is done", rid);
		}
	}

	private void query() {
		LinkedList<String> partitionList = LocksUtil.getPartitions(this.partitions);
		for (String string : partitionList) {
			if (!shutdown) {
				String tableName = table + Config.SEPARATOR + string;
				if (Lucene.INDEXWRITERMAP.containsKey(tableName)) {
					LuceneReader luceneReader = ReaderHandler.getReader(tableName);
					DirectoryReader reader = luceneReader.reader;
					IndexSearcher searcher = new IndexSearcher(reader);
					int num;
					try {
						num = searcher.count(query);
					} catch (IOException e) {
						logger.error("rid " + rid + " - " + e.toString());
						luceneReader.release();
						continue;
					}
					StringBuilder sb = new StringBuilder();
					String result = LocksUtil.stringAppend(sb, System.currentTimeMillis(), "\t3\t", rid, "\t", tableName, "\t", num);
					byte[] bytes = result.getBytes(Config.DEFAULTCHARSET);
					ctx.writeAndFlush(new LocksMsg(bytes.length, bytes));
					luceneReader.release();
				} else
					logger.error("rid {} , Lucene's indexWriterMap does not contain {}", rid, tableName);
			}
		}
	}

}
