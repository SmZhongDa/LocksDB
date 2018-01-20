package com.fiberhome.locksdb.jetty;

import java.util.ArrayList;
import java.util.List;

public class LocksNodeInfo {
	//操作系统基本信息
	
	private String systemInfo;
	
	private String ip ;



	private String memInfo;
	

	private String cpuType;
	
	private String cpuPhysical;
	
	private String cpuThread;
	
	
	//locksdb 基本信息
	private int days;
	private int platformDatabase;
	
	
	//操作系统/locksdb 动态信息
	private List<String> cpuList = new ArrayList<String>();

	private List<String> ioList = new ArrayList<String>();
	

	private List<String> memList = new ArrayList<String>();
	
	public List<String> t = new ArrayList<String>();
	
	
	private List<String> overList = new ArrayList<String>();
	
	private List<String> queryList = new ArrayList<String>();
	private List<String> loaderList = new ArrayList<String>();
	

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public void setT(List<String> timeList) {
		this.t = timeList;
	}
	
	public void setCpuList(List<String> cpuList) {
		this.cpuList = cpuList;
	}

	public void setIoList(List<String> ioList) {
		this.ioList = ioList;
	}

	public void setMemList(List<String> memList) {
		this.memList = memList;
	}

	public void setOverList(List<String> overList) {
		this.overList = overList;
	}
	
	public void setSystemInfo(String systemInfo) {
		this.systemInfo = systemInfo;
	}

	
	public void setMemInfo(String memInfo) {
		this.memInfo = memInfo;
	}

	public void setCpuType(String cpuType) {
		this.cpuType = cpuType;
	}
	
	public String getCpuPhysical() {
		return cpuPhysical;
	}
	
	public void setCpuPhysical(String cpuPhysical) {
		this.cpuPhysical = cpuPhysical;
	}
	

	
	public void setCpuThread(String cpuThread) {
		this.cpuThread = cpuThread;
	}
	
	
	public int getDays() {
		return days;
	}


	public void setDays(int days) {
		this.days = days;
	}


	public int getPlatformDatabase() {
		return platformDatabase;
	}


	public void setPlatformDatabase(int platformDatabase) {
		this.platformDatabase = platformDatabase;
	}


	public List<String> getQueryList() {
		return queryList;
	}


	public void setQueryList(List<String> queryList) {
		this.queryList = queryList;
	}


	public List<String> getLoaderList() {
		return loaderList;
	}


	public void setLoaderList(List<String> loaderList) {
		this.loaderList = loaderList;
	}


	public String getSystemInfo() {
		return systemInfo;
	}


	public String getMemInfo() {
		return memInfo;
	}


	public String getCpuType() {
		return cpuType;
	}


	public String getCpuThread() {
		return cpuThread;
	}


	public List<String> getCpuList() {
		return cpuList;
	}


	public List<String> getIoList() {
		return ioList;
	}


	public List<String> getMemList() {
		return memList;
	}


	public List<String> getOverList() {
		return overList;
	}
	
	public List<String> getT() {
		return t;
	}

}
