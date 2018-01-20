package com.fiberhome.locksdb.query;

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.RandomSamplingFacetsCollector;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.client.LocksMsg;
import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.index.LuceneReader;
import com.fiberhome.locksdb.index.LuceneTaxoReader;
import com.fiberhome.locksdb.index.ReaderHandler;
import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.ConfigLoader;
import com.fiberhome.locksdb.util.LocksUtil;

public class DimensionQuery extends LocksQuery implements Runnable {

	private Logger logger = LoggerFactory.getLogger(DimensionQuery.class);
	final Query query;
	final String partitions;
	final String table;
	final ChannelHandlerContext ctx;
	public final CountDownLatch latch;
	final int sampleSize;

	public DimensionQuery(String query, String table, String partitions, ChannelHandlerContext ctx, CountDownLatch latch, int sampleSize, String rid) throws ParseException {
		super(rid);
		QueryParser qp = new QueryParser("", Config.defaultAnalyzer());
		qp.setLowercaseExpandedTerms(false);
		qp.setAllowLeadingWildcard(true);
		this.query = qp.parse(query);
		this.partitions = partitions;
		this.table = table;
		this.ctx = ctx;
		this.latch = latch;
		this.sampleSize = sampleSize;
	}

	private void query() {
		LinkedList<String> partitionList = LocksUtil.getPartitions(this.partitions);
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (String string : partitionList) {
			if (!shutdown) {
				String tableName = table + Config.SEPARATOR + string;
				if (Lucene.TAXOWRITERMAP.containsKey(tableName)) {
					LuceneTaxoReader taxoReader = ReaderHandler.getTaxoReader(tableName);
					LuceneReader luceneReader = ReaderHandler.getReader(tableName);
					IndexSearcher searcher = new IndexSearcher(luceneReader.reader);
					FacetsCollector fc = new RandomSamplingFacetsCollector(sampleSize);
					try {
						searcher.search(query, fc);
						Facets facets = new FastTaxonomyFacetCounts(taxoReader.reader, ConfigLoader.CONFIG, fc);
						List<FacetResult> dims = facets.getAllDims(Integer.MAX_VALUE);
						if (null != dims) {
							for (FacetResult r : dims) {
								for (LabelAndValue v : r.labelValues) {
									if (map.containsKey(v.label)) {
										int n = map.get(v.label) + v.value.intValue();
										map.put(v.label, n);
									} else
										map.put(v.label, v.value.intValue());
								}
							}
						}
					} catch (IOException e) {
						logger.error(e.toString());
					}
					taxoReader.release();
					luceneReader.release();
				}
			} else
				return;
		}
		StringBuilder sb = new StringBuilder();
		sb.append(System.currentTimeMillis()).append("\t4\t").append(rid);
		for (String __s : map.keySet()) {
			sb.append("\t").append(__s).append("\t").append(map.get(__s));
		}
		byte[] bytes = sb.toString().getBytes(Config.DEFAULTCHARSET);
		ctx.writeAndFlush(new LocksMsg(bytes.length, bytes));
	}

	@Override
	public void run() {
		try {
			query();
		} finally {
			latch.countDown();
			isDone = true;
			logger.info("DimensionQuery {} {} is done", rid, table);
		}
	}

	public static void main(String[] args) {
		List<String> list = new LinkedList<String>();
		String[] arr = list.toArray(new String[0]);
		System.out.println(Arrays.toString(arr));
		System.out.println(arr.length);
	}
}
