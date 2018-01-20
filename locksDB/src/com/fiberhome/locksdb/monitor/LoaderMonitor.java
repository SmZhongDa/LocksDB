package com.fiberhome.locksdb.monitor;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.loader.FileReader;
import com.fiberhome.locksdb.loader.LuceneLoader;
import com.fiberhome.locksdb.loader.RocksLoader;

public class LoaderMonitor {

	private static Logger logger = LoggerFactory.getLogger(LoaderMonitor.class);
	private final FileReader reader;
	private final RocksLoader rocksLoader;
	private final LuceneLoader luceneLoader;
	private final int period;

	public LoaderMonitor(FileReader reader, RocksLoader rocksLoader, LuceneLoader luceneLoader, int period) {
		this.reader = reader;
		this.rocksLoader = rocksLoader;
		this.luceneLoader = luceneLoader;
		this.period = period;
	}

	public void run() {
		ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
		service.scheduleAtFixedRate(new Runnable() {

			private long readerTmp;
			private long rocksTmp;
			private long luceneTmp;

			@Override
			public void run() {
				long readerDiff = reader.getCounter() - readerTmp;
				long rocksDiff = rocksLoader.getCounter() - rocksTmp;
				long luceneDiff = luceneLoader.getCounter() - luceneTmp;
				logger.info("{}\n{}", "DataLoader Stats", "------------------------------------------------------------------------------------------------------------------------------------------------------");
				logger.info("Interval : {} seconds ", period);
				logger.info("FileReader : [ read rows : {} , speed : {} rows/second ]", readerDiff, readerDiff / period);
				logger.info("RocksLoader : [ load rows : {} , speed : {} rows/second ]", rocksDiff, rocksDiff / period);
				logger.info("LuceneLoader : [ load rows : {} , speed : {} rows/second ]\n{}", luceneDiff, luceneDiff / period,
						"------------------------------------------------------------------------------------------------------------------------------------------------------");
				readerTmp = reader.getCounter();
				rocksTmp = rocksLoader.getCounter();
				luceneTmp = luceneLoader.getCounter();
			}
		}, period, period, TimeUnit.SECONDS);

	}

}
