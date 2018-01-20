package com.fiberhome.locksdb;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.PropertyConfigurator;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.data.Rocks;
import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.jetty.JettyStart;
import com.fiberhome.locksdb.loader.DataLoader;
import com.fiberhome.locksdb.scheduler.PartitionMaintainer;
import com.fiberhome.locksdb.scheduler.ReaderRelease;
import com.fiberhome.locksdb.scheduler.ReaderReloader;
import com.fiberhome.locksdb.scheduler.TaxoReaderRelease;
import com.fiberhome.locksdb.scheduler.TaxoReaderReloader;
import com.fiberhome.locksdb.server.LocksServer;
import com.fiberhome.locksdb.util.ConfigLoader;
import com.fiberhome.locksdb.util.MetaDataLoader;

public class LocksMain {

	private static Logger logger = LoggerFactory.getLogger(LocksMain.class);

	public static void main(String[] args) {
		PropertyConfigurator.configure("conf/log4j.properties");
		logger.info("\n------------------------------------------------------------------------------------------------------------------------------------------------------"
				+ "\n------------------------------------------------------------------------------------------------------------------------------------------------------"
				+ "\n------------------------------------------------------------------------------------------------------------------------------------------------------");
		Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			@Override
			public void uncaughtException(Thread t, Throwable e) {
				logger.error("{} - {}\n{}", t.toString(), e.getMessage(), e.getStackTrace());
			}

		});
		open();
	}

	private static void open() {
		logger.info("open locksDB ......");
		ConfigLoader rc = new ConfigLoader();
		logger.info(rc.toString());
		MetaDataLoader metaDataLoader = new MetaDataLoader();
		metaDataLoader.load();
		Lucene lucene = new Lucene(MetaDataLoader.INDEXMAP, rc.getIndexPath());
		Rocks rocks = null;
		try {
			rocks = new Rocks(rc.getDataPath());
			rocks.open();
			lucene.open();
		} catch (RocksDBException | IOException e ) {
			logger.error("open DB failed : {}", e.toString());
			System.exit(1);
		} 
		
		
		try {
			logger.info("start jetty monitor ......");
			JettyStart jettystart = new JettyStart(rc);
			jettystart.start();
		} catch (Exception e) {
			logger.error("start jetty monitor failed : {}", e.toString());
			System.exit(1);
		}
		
		maintain(lucene, rocks);
		load(Paths.get(rc.getDataSource()));
		logger.info("LocksServer start");
		LocksServer locks = new LocksServer(rc.getPort());
		new Thread(locks).start();
	}

	private static void maintain(Lucene lucene, Rocks rocks) {
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT+08:00"));
		int hour = cal.get(Calendar.HOUR_OF_DAY);
		int minute = cal.get(Calendar.MINUTE);
		int second = cal.get(Calendar.SECOND);
		int seconds = hour * 60 * 60 + minute * 60 + second;
		int delay = 86400 - seconds + 3600;
		ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
		int _i = 86400;
		logger.info("Partition Maintainer : [ delay : {} seconds , interval : {} seconds ]", delay, _i);
		service.scheduleAtFixedRate(new PartitionMaintainer(lucene, rocks), delay, _i, TimeUnit.SECONDS);
		int reloader = Integer.parseInt(ConfigLoader.CONFIGMAP.get("reloader"));
		logger.info("Index Reloader : [ delay : {} seconds , interval : {} seconds ]", reloader, reloader);
		service.scheduleWithFixedDelay(new ReaderReloader(), reloader, reloader, TimeUnit.SECONDS);
		int release = Integer.parseInt(ConfigLoader.CONFIGMAP.get("release"));
		logger.info("Index Release : [ delay : {} seconds , interval : {} seconds ]", reloader / 2 + release, release);
		service.scheduleWithFixedDelay(new ReaderRelease(), reloader / 2 + release, release, TimeUnit.SECONDS);
		// TODO
		if (ConfigLoader.CONFIGMAP.get("dimension").equals("1")) {
			int taxoReloader = Integer.parseInt(ConfigLoader.CONFIGMAP.get("taxoRelease"));
			logger.info("TaxoIndex Reloader : [ delay : {} seconds , interval : {} seconds ]", taxoReloader, taxoReloader);
			service.scheduleWithFixedDelay(new TaxoReaderReloader(), taxoReloader, taxoReloader, TimeUnit.SECONDS);
			int taxoRelease = Integer.parseInt(ConfigLoader.CONFIGMAP.get("taxoRelease"));
			logger.info("TaxoIndex Release : [ delay : {} seconds , interval : {} seconds ]", taxoReloader / 2 + taxoRelease, taxoRelease);
			service.scheduleWithFixedDelay(new TaxoReaderRelease(), taxoReloader / 2 + taxoRelease, taxoRelease, TimeUnit.SECONDS);
		}
	}

	private static void load(Path path) {
		new DataLoader(path).run();
	}

}
