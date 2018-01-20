package com.fiberhome.locksdb.index;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;

public class ReaderHandler {

	public static LinkedBlockingQueue<LuceneReader> DEPRECATEDQUEUE = new LinkedBlockingQueue<LuceneReader>();
	public static LinkedBlockingQueue<LuceneTaxoReader> TAXODEPRECATEDQUEUE = new LinkedBlockingQueue<LuceneTaxoReader>();
	private static byte[] lock0 = new byte[0];
	private static byte[] lock1 = new byte[0];

	public static LuceneReader getReader(String key) {
		synchronized (lock0) {
			LuceneReader luceneReader = Lucene.INDEXREADERMAP.get(key);
			luceneReader.count();
			return luceneReader;
		}
	}

	public static void release(Logger logger, LuceneReader luceneReader) throws IOException {
		synchronized (lock0) {
			if (luceneReader.getCounter() == 0) {
				long time = System.currentTimeMillis();
				logger.debug("releasing index reader : {} ......", luceneReader.key);
				luceneReader.reader.close();
				logger.info("released index reader : {} , elapsed time : {}", luceneReader.key, System.currentTimeMillis() - time);
			}
		}
	}

	public static LuceneTaxoReader getTaxoReader(String key) {
		synchronized (lock1) {
			LuceneTaxoReader taxoReader = Lucene.TAXOREADERMAP.get(key);
			taxoReader.count();
			return taxoReader;
		}
	}

	public static void release(Logger logger, LuceneTaxoReader taxoReader) throws IOException {
		synchronized (lock1) {
			if (taxoReader.getCounter() == 0) {
				long time = System.currentTimeMillis();
				logger.debug("releasing taxo index reader : {} ......", taxoReader.key);
				taxoReader.reader.close();
				logger.info("released taox index reader : {} , elapsed time : {}", taxoReader.key, System.currentTimeMillis() - time);
			}
		}
	}

}
