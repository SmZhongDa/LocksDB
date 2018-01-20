package com.fiberhome.locksdb.scheduler;

import java.io.IOException;
import java.util.List;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.data.Rocks;
import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.util.LocksUtil;

public class PartitionMaintainer implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(PartitionMaintainer.class);
	private final Lucene lucene;
	private final Rocks rocks;

	public PartitionMaintainer(Lucene lucene, Rocks rocks) {
		this.lucene = lucene;
		this.rocks = rocks;
	}

	private void dropPartition() {
		logger.info("partition maintaining ......");
		List<String> days = LocksUtil.getDays();
		logger.info("valid date : " + days);
		logger.info("see locksDB_data.log & locksDB_index.log for details");
		try {
			lucene.reopen(days);
			rocks.reopen(days);
		} catch (IOException | IllegalArgumentException | RocksDBException e) {
			logger.error("maintain partition failed : " + e.toString());
			try {
				lucene.close();
			} catch (IOException ioe) {
				logger.error(ioe.toString());
			}
			rocks.close();
			System.exit(1);
		}
		logger.info("maintain partition successfully");
	}

	@Override
	public void run() {
		logger.info("Partition Maintainer running ......");
		dropPartition();
		logger.info("Partition Maintainer finished");
	}

}
