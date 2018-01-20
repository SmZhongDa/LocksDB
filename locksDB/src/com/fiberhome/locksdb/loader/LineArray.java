package com.fiberhome.locksdb.loader;

import java.util.Objects;

public class LineArray {

	public final String[] array;

	LineArray(String[] array) {
		this.array = Objects.requireNonNull(array, "string array is null");
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(array[0]);
		for (int i = 1; i < array.length; i++) {
			sb.append("\t").append(array[i]);
		}
		return sb.toString();
	}

}
