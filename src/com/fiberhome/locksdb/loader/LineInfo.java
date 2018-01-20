package com.fiberhome.locksdb.loader;

public class LineInfo<T, L, V> {

	public final T tableName;
	public final L locksID;
	public final V value;

	public LineInfo(T tableName, L locksID, V value) {
		this.tableName = tableName;
		this.locksID = locksID;
		this.value = value;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[ table name : ");
		sb.append(tableName);
		sb.append(" , locksID : ");
		sb.append(locksID);
		sb.append(" , value : ");
		sb.append(value);
		sb.append(" ]");
		return sb.toString();
	}

}
