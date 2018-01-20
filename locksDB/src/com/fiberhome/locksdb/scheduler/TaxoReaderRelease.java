package com.fiberhome.locksdb.scheduler;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.index.LuceneTaxoReader;
import com.fiberhome.locksdb.index.ReaderHandler;

public class TaxoReaderRelease implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(TaxoReaderRelease.class);

	@Override
	public void run() {
		release();
	}

	private void release() {
		LuceneTaxoReader reader;
		logger.debug("TAXODEPRECATEDQUEUE size : {}", ReaderHandler.TAXODEPRECATEDQUEUE.size());
		while ((reader = ReaderHandler.TAXODEPRECATEDQUEUE.poll()) != null) {
			try {
				ReaderHandler.release(logger, reader);
			} catch (IOException e) {
				logger.error(e.toString());
			}
			logger.debug("TAXODEPRECATEDQUEUE size : {}", ReaderHandler.TAXODEPRECATEDQUEUE.size());
		}
	}

}
