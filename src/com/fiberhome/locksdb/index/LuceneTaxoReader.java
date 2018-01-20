package com.fiberhome.locksdb.index;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.facet.taxonomy.TaxonomyReader;

public class LuceneTaxoReader {

	public final String key;
	public final TaxonomyReader reader;
	private AtomicInteger counter = new AtomicInteger(0);

	public LuceneTaxoReader(String key, TaxonomyReader reader) {
		this.key = key;
		this.reader = reader;
	}

	public int count() {
		return counter.incrementAndGet();
	}

	public int release() {
		return counter.addAndGet(-1);
	}

	public int getCounter() {
		return counter.get();
	}

}
