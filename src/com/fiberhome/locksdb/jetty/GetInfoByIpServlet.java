package com.fiberhome.locksdb.jetty;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GetInfoByIpServlet extends HttpServlet {
	private Logger logger = LoggerFactory.getLogger(GetInfoByIpServlet.class);
	
	private static final long serialVersionUID = 1L;
	private List<String> listTime = new ArrayList<String>();

	public GetInfoByIpServlet() {
		super();
	}
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) {
		this.doPost(request, response);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		try{
			String ip = request.getParameter("ip");
			LocksNodeInfo node = new LocksNodeInfo();
			listTime = DataInduction.listTime;
			List<String> listCpu= DataInduction.listCpu;
			List<String> listIo= DataInduction.listIo;
			List<String> listMem= DataInduction.listMem;
			List<String> listOver= DataInduction.listOver;
			List<String> listQuery= DataInduction.listQuery;
			List<String> listLoder= DataInduction.listLoder;
			
			
			node.setIp(ip);
			node.setT(listTime);
			node.setCpuList(listCpu);
			node.setIoList(listIo);
			node.setMemList(listMem);
			node.setOverList(listOver);
			node.setLoaderList(listLoder);
			node.setQueryList(listQuery);
			
			node.setSystemInfo(DataInduction.SystemInfo);
			node.setMemInfo(DataInduction.MemInfo);
			node.setCpuPhysical(DataInduction.CpuPhysical);
			node.setCpuThread(DataInduction.CpuThread);
			node.setCpuType(DataInduction.CpuType);
			
			
			request.setAttribute("node", node);
			request.getRequestDispatcher("/locksdb/node.jsp").forward(request, response);
			
			
		}catch(Exception e){
			logger.error("doPost procedure failed : {}", e.toString());
		}
	
	}

}
