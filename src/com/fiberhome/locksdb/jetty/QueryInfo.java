package com.fiberhome.locksdb.jetty;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class QueryInfo {

	private ConcurrentHashMap<String,List<HashMap<String, String>>> resultMap;
	private ConcurrentHashMap<String,String> resultCount;

	private ConcurrentHashMap<String,String> timeMap;
	private String l;
	
	
	
	public String getL() {
		return l;
	}
	public void setL(String l) {
		this.l = l;
	}
	public ConcurrentHashMap<String, String> getTimeMap() {
		return timeMap;
	}
	public void setTimeMap(ConcurrentHashMap<String, String> timeMap) {
		this.timeMap = timeMap;
	}
	public ConcurrentHashMap<String, List<HashMap<String, String>>> getResultMap() {
		return resultMap;
	}
	public void setResultMap(ConcurrentHashMap<String, List<HashMap<String, String>>> resultMap) {
		this.resultMap = resultMap;
	}


	public ConcurrentHashMap<String,String> getResultCount() {
		return resultCount;
	}
	public void setResultCount(ConcurrentHashMap<String, String> resultCount) {
		this.resultCount = resultCount;
	}
	

}
