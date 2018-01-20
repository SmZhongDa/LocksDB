package com.fiberhome.locksdb.loader;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.monitor.LoaderMonitor;
import com.fiberhome.locksdb.util.ConfigLoader;
import com.fiberhome.locksdb.util.MetaDataLoader;

public class DataLoader {

	private Logger logger = LoggerFactory.getLogger(DataLoader.class);
	public final FileReader reader;
	private ExecutorService exec;

	public DataLoader(Path path) {
		this.reader = new FileReader(MetaDataLoader.INDEXMAP, path);
	}

	/**
	 * @return the isDone
	 */
	public boolean isDone() {
		if (exec.isTerminated())
			return true;
		return false;
	}

	public void shutdown() {
		reader.shutdown();
	}

	public void run() {
		logger.info("DataLoader start");
		LinkedBlockingQueue<String> errorData = new LinkedBlockingQueue<String>(10000);
		RocksLoader rocksLoader = new RocksLoader(reader, errorData);
		LuceneLoader luceneLoader = null;
		if (ConfigLoader.CONFIGMAP.get("dimension").equals("1"))
			luceneLoader = new DimensionLoader(rocksLoader, rocksLoader.successData, errorData);
		else
			luceneLoader = new LuceneLoader(rocksLoader, rocksLoader.successData, errorData);
		logger.info("LoaderMonitor start");
		new LoaderMonitor(reader, rocksLoader, luceneLoader, Integer.parseInt(ConfigLoader.CONFIGMAP.get("period"))).run();
		logger.info("error data FileWriter start");
		new Thread(new FileWriter(errorData)).start();
		exec = Executors.newCachedThreadPool();
		logger.info("FileReader start");
		exec.execute(reader);
		logger.info("RocksLoader start");
		exec.execute(rocksLoader);
		logger.info("LuceneLoader start");
		exec.execute(luceneLoader);
		logger.info("DataLoader running ......");
		exec.shutdown();
	}

}
