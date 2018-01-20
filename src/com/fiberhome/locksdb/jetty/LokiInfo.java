package com.fiberhome.locksdb.jetty;

import java.util.HashMap;
import java.util.List;

public class LokiInfo {
	private String num;
	private List<String> listIp;
	private String queryNum;
	private String mtainNum;
	private String mtainSuccessfulNum;

	private HashMap<String,HashMap<String,List<String>>> nodeMap = new HashMap<String,HashMap<String,List<String>>>();
	
	
	
	public String getMtainSuccessfulNum() {
		return mtainSuccessfulNum;
	}

	public void setMtainSuccessfulNum(String mtainSuccessfulNum) {
		this.mtainSuccessfulNum = mtainSuccessfulNum;
	}
	
	
	public HashMap<String, HashMap<String, List<String>>> getNodeMap() {
		return nodeMap;
	}

	public void setNodeMap(HashMap<String, HashMap<String, List<String>>> nodeMap) {
		this.nodeMap = nodeMap;
	}

	public String getMtainNum() {
		return mtainNum;
	}

	public void setMtainNum(String mtainNum) {
		this.mtainNum = mtainNum;
	}

	public String getQueryNum() {
		return queryNum;
	}

	public void setQueryNum(String queryNum) {
		this.queryNum = queryNum;
	}

	public List<String> getListIp() {
		return listIp;
	}

	public void setListIp(List<String> listIp) {
		this.listIp = listIp;
	}

	public String getNum() {
		return num;
	}

	public void setNum(String num) {
		this.num = num;
	}

}
