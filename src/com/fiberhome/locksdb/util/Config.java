package com.fiberhome.locksdb.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;

public class Config {

	public static final String LCOKSTABLE = "LOCKSTABLE";

	public static final String _ID = "_id";

	public static final String PREFIX = "LOCKSTABLE_";

	public static final Charset DEFAULTCHARSET = StandardCharsets.UTF_8;

	public static final String CAPTURETIME = "CAPTURE_TIME";
	public static final String DEFAULTORDERBY = "CAPTURE_TIME";
	
	public static final boolean DESC = true;
	public static final boolean ASC = false;
	public static final String SEPARATOR = "_";

	public static Analyzer defaultAnalyzer() {
		return new KeywordAnalyzer();
	}

	private Config() {
	}

}
