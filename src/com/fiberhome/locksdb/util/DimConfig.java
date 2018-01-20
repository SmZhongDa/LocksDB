package com.fiberhome.locksdb.util;

import java.util.Arrays;

public class DimConfig {

	public final String name;
	public final int[] offset;
	public final String separator;

	public DimConfig(String name, int[] offset, String separator) {
		this.name = name;
		this.offset = offset;
		this.separator = separator;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[ name : ").append(name).append(" , offset : ").append(Arrays.toString(offset)).append(" , separator : ").append(separator).append(" ]");
		return sb.toString();
	}

}
