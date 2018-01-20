package com.fiberhome.locksdb.jetty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GetLokiInfoServlet extends HttpServlet{
	private Logger logger = LoggerFactory.getLogger(GetLokiInfoServlet.class);
	private static final long serialVersionUID = 5810120822959470622L;
	private String logPath = "/home/nebula/app_loki/log/Loki_LocksSQL.log";
	
	private String lokiIp = JettyStart.rc.getLokiIp();
	private int port = JettyStart.rc.getPort();
	
	public GetLokiInfoServlet(){
		super();
	}
	
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) {
		this.doPost(request, response);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		try {
			request.setCharacterEncoding("utf-8");
			response.setCharacterEncoding("utf-8");
			
			LokiInfo loki = new LokiInfo();
			
			String ConnNum = getConnectionNum(lokiIp);
			List<String> listIp = getConnectionIp(lokiIp);
			String queryNum = getQueryNum(lokiIp);
			String mtainNum = getMtainNum(lokiIp);
			String mtainSuccessfulNum = getSuccessfulMtainNum(lokiIp);
			HashMap<String,HashMap<String,List<String>>> nodeMap = getNodeInfo(lokiIp,listIp);
		
			loki.setNum(ConnNum);
			loki.setListIp(listIp);
			loki.setQueryNum(queryNum);
			loki.setMtainNum(mtainNum);
			loki.setMtainSuccessfulNum(mtainSuccessfulNum);
			loki.setNodeMap(nodeMap);
			
			request.setAttribute("loki", loki);
			request.getRequestDispatcher("/loki/loki.jsp").forward(request, response);

		} catch (Exception e) {
			logger.error("procedure failed : {}", e.toString());
		}

	}
	
	
	public String getConnectionNum(String ip) throws Exception{
		String cmd = "netstat -an | grep " + port + " | grep -c ESTABLISHED";
		RmtShellExecutor exe = new RmtShellExecutor(ip,"locksuser","123456");
		String result = exe.exec(cmd);
		return result;
	}
	
	
	public List<String> getConnectionIp(String ip) throws Exception{
		String cmd = "netstat -an | grep " + port + " | grep  ESTABLISHED | awk '{print $5}' | awk -F\":\" '{print $4\"ll\"}'";
		RmtShellExecutor exe = new RmtShellExecutor(ip,"locksuser","123456");
		String result = exe.exec(cmd);
		String[] line = result.trim().split("ll");
		List<String> listT = JettyTolls.parserLine(line,0);
		return listT;
	}
	
	
	
	public String getQueryNum(String ip) throws Exception{
		String cmd = "grep -c \"receive LocksReq\" " + logPath + "";
		RmtShellExecutor exe = new RmtShellExecutor(ip,"locksuser","123456");
		String result = exe.exec(cmd);
		return result;
	}

	public String getMtainNum(String ip) throws Exception{;
		String cmd = "grep \"[keepAlive]\" " + logPath + "  | grep -c  down";
		RmtShellExecutor exe = new RmtShellExecutor(ip,"locksuser","123456");
		String result = exe.exec(cmd);
		return result;
	}
	
	
	public String getSuccessfulMtainNum(String ip) throws Exception{
		String cmd = "grep \"[badkeepAlive]\" " + logPath + " | grep -c successfully";
		RmtShellExecutor exe = new RmtShellExecutor(ip,"locksuser","123456");
		String result = exe.exec(cmd);
		return result;
	}
	
	public HashMap<String,HashMap<String,List<String>>> getNodeInfo(String ip,List<String> listIp) throws Exception{
		HashMap<String,HashMap<String,List<String>>> totalMap = new HashMap<String,HashMap<String,List<String>>>();
		for(String ip1:listIp){
			HashMap<String,List<String>> tempMap = new HashMap<String,List<String>>();
			ArrayList<String> numList = new ArrayList<String>();
			
			String cmd = "grep \"may be timeout\" " + logPath + " | grep -c " + ip1 ;
			RmtShellExecutor exe = new RmtShellExecutor(ip,"locksuser","123456");
			String times = exe.exec(cmd);
			numList.add(times);
			tempMap.put("num", numList);
			
			String cmd1 = "grep \"locksdb server : " + ip1 + " -- cost time\" " + logPath + " | awk '{print $12$13\"ll\"}'";
			RmtShellExecutor exe1 = new RmtShellExecutor(ip,"locksuser","123456");
			String result = exe1.exec(cmd1);
			String[] line = result.trim().split("ll");
			List<String> timesList = JettyTolls.parserLine(line,1);
			tempMap.put("time", timesList);
			totalMap.put(ip1, tempMap);
		}
		return totalMap;
	}
}
