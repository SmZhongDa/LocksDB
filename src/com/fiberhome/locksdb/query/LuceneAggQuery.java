package com.fiberhome.locksdb.query;

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
import org.apache.lucene.search.TotalHitCountCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.index.LuceneReader;
import com.fiberhome.locksdb.index.ReaderHandler;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.LocksUtil;

public class LuceneAggQuery extends LocksQuery implements Runnable {

	private Logger logger = LoggerFactory.getLogger(LuceneAggQuery.class);
	private final LinkedBlockingQueue<Pair<String, byte[]>> keyQueue;
	private final Query query;
	private final boolean orderby;
	private final String partitions;
	private final String table;
	private final CountDownLatch latch;
	private int put = 0;

	public LuceneAggQuery(CountDownLatch latch, String query, Boolean orderby , LinkedBlockingQueue<Pair<String, byte[]>> keyQueue,String partitions, String table, String rid)
			throws ParseException {
		super(rid);
		this.latch = latch;
		QueryParser qp = new QueryParser("", Config.defaultAnalyzer());
		qp.setLowercaseExpandedTerms(false);
		qp.setAllowLeadingWildcard(true);
		this.query = qp.parse(query);
		this.orderby = orderby;
		this.keyQueue = keyQueue;
		this.partitions = partitions;
		this.table = table;
	}

	private void query() {
		LinkedList<String> partitionList = LocksUtil.getPartitions(this.partitions);
		List<LuceneReader> readerList = new LinkedList<LuceneReader>();
		for (String string : partitionList) {
			if (!shutdown) {
				String tableName = table + Config.SEPARATOR + string;
				if (Lucene.INDEXWRITERMAP.containsKey(tableName)) {
					LuceneReader luceneReader = ReaderHandler.getReader(tableName);
					readerList.add(luceneReader);
				} else
					logger.error("rid {} , Lucene's indexWriterMap does not contain {}",rid, tableName);
			} else
				break;
		}

		for (LuceneReader luceneReader : readerList) {
			DirectoryReader reader = luceneReader.reader;
			IndexSearcher searcher = new IndexSearcher(reader);
			TopDocs docs;
			try {
				TotalHitCountCollector counter = new TotalHitCountCollector();
				searcher.search(query,counter);
				logger.debug(counter.getTotalHits() + " ");
				if (orderby)
					docs =  searcher.search(query,counter.getTotalHits() + 1,new Sort(new SortField(Config.DEFAULTORDERBY, SortField.Type.STRING, Config.DESC)));
				else
					docs =  searcher.search(query,counter.getTotalHits() + 1);
			} catch (IOException e) {
				logger.error("rid " + rid + " - " + e.toString());
				luceneReader.release();
				continue;
			}
			for (ScoreDoc d : docs.scoreDocs) {
				try {
					long __l = searcher.doc(d.doc).getField(Config._ID).numericValue().longValue();
					keyQueue.put(new Pair<String, byte[]>(luceneReader.key.split(Config.SEPARATOR, -1)[1], LocksUtil.longToBytes(__l)));
					put++;
				} catch (IOException | InterruptedException e) {
					logger.error("rid " + rid + " - " + e.toString());
					break;
				}
			}
			
			luceneReader.release();
		}
	}

	@Override
	public void run() {
		try {
			logger.debug("enter into LuceneAggQuery run.........");
			query();
		} finally {
			latch.countDown();
			isDone = true;
			logger.info("LuceneQuery {} put : {}", rid, put);
		}
	}

}
