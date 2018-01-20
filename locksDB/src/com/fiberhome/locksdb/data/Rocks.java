package com.fiberhome.locksdb.data;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.IndexType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.ConfigLoader;
import com.fiberhome.locksdb.util.LocksUtil;

public class Rocks {

	private static Logger logger = LoggerFactory.getLogger(Rocks.class);
	public static ConcurrentHashMap<String, DBInfo> DBINFOS = new ConcurrentHashMap<String, DBInfo>();;
	private final String WALPath;
	private final String dbPath;

	public Rocks(String dbPath) throws IOException {
		if (!dbPath.endsWith("/"))
			this.dbPath = dbPath + "/";
		else
			this.dbPath = dbPath;
		String tmp = ConfigLoader.CONFIGMAP.get("WALPath");
		if (!dbPath.endsWith("/"))
			this.WALPath = tmp + "/";
		else
			this.WALPath = tmp;
		createDirectory(Paths.get(WALPath));
		createDirectory(Paths.get(dbPath));
	}

	private void createDirectory(Path path) throws IOException {
		if (Files.exists(path) && !Files.isDirectory(path))
			throw new IOException("file " + path + " exists and is not a directory");
		if (!Files.exists(path)) {
			Files.createDirectory(path);
		}
	}

	private void openDB(String date) throws RocksDBException, IOException {
		logger.info("open rocks : {} , column family : {}", date, Config.LCOKSTABLE);
		createDirectory(Paths.get(WALPath + date));
		createDirectory(Paths.get(dbPath + date));
		DBOptions dbops = new DBOptions();
		ColumnFamilyOptions cfops = new ColumnFamilyOptions();
		Options ops = new Options(dbops, cfops);
		String logPath = WALPath + date;
		setParameter(logPath, dbops, cfops, ops);
		HashSet<byte[]> set = new HashSet<byte[]>();
		List<byte[]> cfList = RocksDB.listColumnFamilies(ops, dbPath);
		LinkedList<ColumnFamilyDescriptor> columnFamilyDescriptors = new LinkedList<ColumnFamilyDescriptor>();
		LinkedList<ColumnFamilyHandle> columnFamilyHandles = new LinkedList<ColumnFamilyHandle>();
		set.add(Config.LCOKSTABLE.getBytes());
		if (cfList.size() > 0) {
			// dead code
			for (byte[] bs : cfList) {
				set.add(bs);
			}
		} else {
			columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfops));
		}
		for (byte[] b : set) {
			columnFamilyDescriptors.add(new ColumnFamilyDescriptor(b, cfops));
		}
		columnFamilyHandles = new LinkedList<ColumnFamilyHandle>();
		String dataPath = dbPath + date;
		RocksDB db = RocksDB.open(dbops, dataPath, columnFamilyDescriptors, columnFamilyHandles);
		HashMap<String, ColumnFamilyInfo> columnFamilyInfos = new HashMap<String, ColumnFamilyInfo>();
		for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
			ColumnFamilyDescriptor c = columnFamilyDescriptors.get(i);
			String name = new String(c.columnFamilyName());
			columnFamilyInfos.put(name, new ColumnFamilyInfo(c, columnFamilyHandles.get(i)));
		}
		cfops.close();
		dbops.close();
		ops.close();
		DBINFOS.put(date, new DBInfo(db, columnFamilyInfos, Paths.get(dataPath), Paths.get(logPath)));
	}

	public void open() throws RocksDBException, IOException {
		logger.info("open RocksDB : {}\tWALPath : {}", dbPath, WALPath);
		RocksDB.loadLibrary();
		for (String s : LocksUtil.getDays()) {
			openDB(s);
		}
		logger.info("open RocksDB successfully");
	}

	public void reopen(List<String> list) throws IllegalArgumentException, RocksDBException, IOException {
		logger.info("RocksDB maintainer running ......");
		Iterator<String> it = DBINFOS.keySet().iterator();
		while (it.hasNext()) {
			String time = it.next();
			if (!list.contains(time)) {
				logger.info("drop database : {}", time);
				DBInfo dbInfo = DBINFOS.get(time);
				it.remove();
				// TODO
				// dbInfo.db.dropColumnFamily(dbInfo.columnFamilyInfos.get(Config.LCOKSTABLE).columnFamilyHandle);
				dbInfo.db.close();
				LocksUtil.deleteFiles(dbInfo.dataPath);
				LocksUtil.deleteFiles(dbInfo.logPath);
			}
		}
		for (String s : LocksUtil.getDays()) {
			if (!DBINFOS.containsKey(s))
				openDB(s);
		}
		logger.info("maintain RocksDB successfully");
	}

	private void setParameter(String WALPath, DBOptions dbops, ColumnFamilyOptions cfops, Options ops) {
		dbops.setCreateIfMissing(true);
		dbops.setCreateMissingColumnFamilies(true);
		dbops.setParanoidChecks(false);
		dbops.setIncreaseParallelism(12);
		dbops.setStatsDumpPeriodSec(60);
		dbops.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);
		// WAL路径
		dbops.setWalDir(WALPath);
		dbops.setWalTtlSeconds(60);
		dbops.setWalSizeLimitMB(1 << 18);
		// cfops.setCompactionStyle(CompactionStyle.UNIVERSAL);
		// ops.optimizeUniversalStyleCompaction();
		// GenericRateLimiterConfig rateLimiterConfig = new
		// GenericRateLimiterConfig(48 * 1 << 20);
		// dbops.setRateLimiterConfig(rateLimiterConfig);
		cfops.setCompactionStyle(CompactionStyle.LEVEL);
		cfops.setLevelZeroSlowdownWritesTrigger(1 << 4);
		cfops.setLevelZeroStopWritesTrigger(1 << 5);
		cfops.setLevelZeroFileNumCompactionTrigger(1 << 1);
		cfops.setMaxWriteBufferNumber(1 << 4);
		cfops.setWriteBufferSize(1 << 25);
		// LZ4+32 ZLIB+16
		cfops.setTargetFileSizeBase(1 << 26);
		cfops.setTargetFileSizeMultiplier(1);
		cfops.setMaxBytesForLevelBase(1 << 29);
		cfops.setMaxBytesForLevelMultiplier(5);
		// cfops.setSourceCompactionFactor(1);
		// cfops.setExpandedCompactionFactor(10);
		// cfops.setMaxGrandparentOverlapFactor(10);
		// cfops.setLevelCompactionDynamicLevelBytes(true);
		List<CompressionType> list = new LinkedList<CompressionType>();
		list.add(CompressionType.NO_COMPRESSION);
		list.add(CompressionType.NO_COMPRESSION);
		list.add(CompressionType.LZ4_COMPRESSION);
		list.add(CompressionType.LZ4_COMPRESSION);
		list.add(CompressionType.LZ4_COMPRESSION);
		list.add(CompressionType.LZ4_COMPRESSION);
		list.add(CompressionType.ZLIB_COMPRESSION);
		cfops.setCompressionPerLevel(list);
		BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
		blockBasedTableConfig.setBlockSize(1 << 16);
		blockBasedTableConfig.setNoBlockCache(true);
		blockBasedTableConfig.setBlockCacheSize(0);
		blockBasedTableConfig.setIndexType(IndexType.kBinarySearch);
		blockBasedTableConfig.setWholeKeyFiltering(true);
		BloomFilter bloomFilter = new BloomFilter(10, true);
		blockBasedTableConfig.setFilter(bloomFilter);
		blockBasedTableConfig.setFormatVersion(2);
		cfops.setTableFormatConfig(blockBasedTableConfig);
	}

	public void close() {
		logger.info("close RocksDB ......");
		for (DBInfo info : DBINFOS.values()) {
			info.db.close();
		}
	}

}
