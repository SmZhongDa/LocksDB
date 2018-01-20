package com.fiberhome.locksdb.query;

public class Pair<K, V> {

	public final K key;
	public final V value;

	public Pair(K key, V value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[ ");
		sb.append(key);
		sb.append(" : ");
		sb.append(value);
		sb.append(" ]");
		return sb.toString();
	}

}
