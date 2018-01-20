package com.fiberhome.locksdb.query;

import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.RandomSamplingFacetsCollector;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
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

public class SubDimensionQuery extends DimensionQuery {

	private Logger logger = LoggerFactory.getLogger(SubDimensionQuery.class);
	private final String dim;
	private final String[] path;

	public SubDimensionQuery(String query, String table, String partitions, ChannelHandlerContext ctx, CountDownLatch latch, int sampleSize, String rid, String dim, String... path) throws ParseException {
		super(query, table, partitions, ctx, latch, sampleSize, rid);
		this.dim = dim;
		this.path = path;
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
						FacetResult dims = facets.getTopChildren(Integer.MAX_VALUE, dim, path);
						if (null != dims) {
							StringBuilder __p = new StringBuilder();
							for (String _s : dims.path) {
								__p.append(_s);
							}
							for (LabelAndValue v : dims.labelValues) {
								String __k = __p.toString() + v.label;
								if (map.containsKey(__k)) {
									int n = map.get(__k) + v.value.intValue();
									map.put(__k, n);
								} else
									map.put(__k, v.value.intValue());
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
			logger.info("subDimensionQuery {} {} is done", rid, table);
		}
	}
}
