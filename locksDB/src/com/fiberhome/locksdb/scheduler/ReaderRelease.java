package com.fiberhome.locksdb.scheduler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.index.LuceneReader;
import com.fiberhome.locksdb.index.ReaderHandler;

public class ReaderRelease implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ReaderRelease.class);

	@Override
	public void run() {
		release();
	}

	private void release() {
		LuceneReader luceneReader;
		logger.debug("DEPRECATEDQUEUE size : {}", ReaderHandler.DEPRECATEDQUEUE.size());
		while ((luceneReader = ReaderHandler.DEPRECATEDQUEUE.poll()) != null) {
			try {
				ReaderHandler.release(logger, luceneReader);
			} catch (IOException e) {
				logger.error(e.toString());
			}
			logger.debug("DEPRECATEDQUEUE size : {}", ReaderHandler.DEPRECATEDQUEUE.size());
		}
	}

}
