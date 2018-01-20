package com.fiberhome.locksdb.data;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;

public class ColumnFamilyInfo {

	public final ColumnFamilyDescriptor columnFamilyDescriptor;
	public final ColumnFamilyHandle columnFamilyHandle;

	public ColumnFamilyInfo(ColumnFamilyDescriptor columnFamilyDescriptor, ColumnFamilyHandle columnFamilyHandle) {
		this.columnFamilyDescriptor = columnFamilyDescriptor;
		this.columnFamilyHandle = columnFamilyHandle;
	}

}
