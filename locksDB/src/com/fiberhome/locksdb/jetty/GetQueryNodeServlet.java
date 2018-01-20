package com.fiberhome.locksdb.jetty;

import java.util.List;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetQueryNodeServlet extends HttpServlet {

	private static final long serialVersionUID = 2155666996343064372L;
	private Logger logger = LoggerFactory.getLogger(GetQueryNodeServlet.class);
	public static List<String> nodeList;
	
	public GetQueryNodeServlet(){
		super();
	}
	
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) {
		this.doPost(request, response);
	}
	
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		try {
			nodeList = JettyTolls.getlocksNode();
			request.setAttribute("nodeList", nodeList);
			request.getRequestDispatcher("/locksdb/query.jsp").forward(request, response);
		} catch (Exception e) {
			logger.error("GetQueryNodeServlet happen error.." + e.toString());
		}
	}
}
