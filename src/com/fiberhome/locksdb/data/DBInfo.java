package com.fiberhome.locksdb.data;

import java.nio.file.Path;
import java.util.HashMap;

import org.rocksdb.RocksDB;

public class DBInfo {

	public final RocksDB db;
	public final HashMap<String, ColumnFamilyInfo> columnFamilyInfos;
	public final Path dataPath;
	public final Path logPath;

	public DBInfo(RocksDB db, HashMap<String, ColumnFamilyInfo> columnFamilyInfos, Path dataPath, Path logPath) {
		this.db = db;
		this.columnFamilyInfos = columnFamilyInfos;
		this.dataPath = dataPath;
		this.logPath = logPath;
	}

}
