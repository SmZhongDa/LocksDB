package com.fiberhome.locksdb.index;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.index.DirectoryReader;

public class LuceneReader {

	public final String key;
	public final DirectoryReader reader;
	private AtomicInteger counter = new AtomicInteger(0);

	public LuceneReader(String key, DirectoryReader reader) {
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
