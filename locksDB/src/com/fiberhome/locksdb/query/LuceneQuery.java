package com.fiberhome.locksdb.query;

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.client.LocksMsg;
import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.index.LuceneReader;
import com.fiberhome.locksdb.index.ReaderHandler;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.LocksUtil;

public class LuceneQuery extends LocksQuery implements Runnable {

	private Logger logger = LoggerFactory.getLogger(LuceneQuery.class);
	private final LinkedBlockingQueue<Pair<String, byte[]>> keyQueue;
	private final Query query;
	private final String partitions;
	private final String table;
	private int returnNumber;
	private final boolean orderby;
	private final boolean count;
	private final CountDownLatch latch;
	private int put = 0;
	private final ChannelHandlerContext ctx;

	public LuceneQuery(CountDownLatch latch, String query, LinkedBlockingQueue<Pair<String, byte[]>> keyQueue, String partitions, String table, int returnNumber, boolean orderby, boolean count, String rid,
			ChannelHandlerContext ctx) throws ParseException {
		super(rid);
		this.latch = latch;
		QueryParser qp = new QueryParser("", Config.defaultAnalyzer());
		qp.setLowercaseExpandedTerms(false);
		qp.setAllowLeadingWildcard(true);
		this.query = qp.parse(query);
		this.keyQueue = keyQueue;
		this.partitions = partitions;
		this.table = table;
		this.returnNumber = returnNumber;
		this.orderby = orderby;
		this.count = count;
		this.ctx = ctx;
	}

	private void query() {
		LinkedList<String> partitionList = LocksUtil.getPartitions(this.partitions);
		List<LuceneReader> readerList = new LinkedList<LuceneReader>();
		long total = 0l;
		for (String string : partitionList) {
			if (!shutdown) {
				String tableName = table + Config.SEPARATOR + string;
				if (Lucene.INDEXWRITERMAP.containsKey(tableName)) {
					LuceneReader luceneReader = ReaderHandler.getReader(tableName);
					readerList.add(luceneReader);
					if (count) {
						IndexSearcher searcher = new IndexSearcher(luceneReader.reader);
						try {
							total += searcher.count(query);
						} catch (IOException e) {
							logger.error("rid " + rid + " - " + e.toString());
						}
					}
				} else
					logger.error("rid {} , Lucene's indexWriterMap does not contain {}", rid, tableName);
			} else
				break;
		}
		if (count && !shutdown) {
			StringBuilder sb = new StringBuilder();
			String result = LocksUtil.stringAppend(sb, System.currentTimeMillis(), "\t3\t", rid, "\t", table, "\t", total);
			byte[] bytes = result.getBytes(Config.DEFAULTCHARSET);
			ctx.writeAndFlush(new LocksMsg(bytes.length, bytes));
		}
		for (LuceneReader luceneReader : readerList) {
			if (!shutdown && returnNumber != 0) {
				DirectoryReader reader = luceneReader.reader;
				IndexSearcher searcher = new IndexSearcher(reader);
				TopDocs docs;
				try {
					if (orderby)
						docs = searcher.search(query, returnNumber, new Sort(new SortField(Config.DEFAULTORDERBY, SortField.Type.STRING, Config.DESC)), false, false);
					else
						docs = searcher.search(query, returnNumber);
				} catch (IOException e) {
					logger.error("rid " + rid + " - " + e.toString());
					luceneReader.release();
					continue;
				}
				for (ScoreDoc d : docs.scoreDocs) {
					if (!shutdown) {
						try {
							long __l = searcher.doc(d.doc).getField(Config._ID).numericValue().longValue();
							keyQueue.put(new Pair<String, byte[]>(luceneReader.key.split(Config.SEPARATOR, -1)[1], LocksUtil.longToBytes(__l)));
							returnNumber--;
							put++;
						} catch (IOException | InterruptedException e) {
							logger.error("rid " + rid + " - " + e.toString());
							break;
						}
					} else
						break;
				}
				luceneReader.release();
			} else
				luceneReader.release();
		}
	}

	@Override
	public void run() {
		try {
			query();
		} finally {
			latch.countDown();
			isDone = true;
			logger.info("LuceneQuery {} put : {}", rid, put);
		}
	}

}
