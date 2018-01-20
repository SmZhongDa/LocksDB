package com.fiberhome.locksdb.jetty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GetInfoServlet extends HttpServlet {
	private Logger logger = LoggerFactory.getLogger(GetInfoServlet.class);
	private static final long serialVersionUID = -6934830786220106937L;
	private List<String> list = new ArrayList<String>();
	private HashMap<String, String> infoMap = new HashMap<String, String>(30);

	public GetInfoServlet() {
		super();
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) {
		this.doPost(request, response);
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		try {
			request.setCharacterEncoding("utf-8");
			response.setCharacterEncoding("utf-8");
			list = JettyTolls.getlocksNode();

			for (String ip : list) {
				String status = isAbleConn(ip);
				infoMap.put(ip, status);
			}
			request.setAttribute("map", infoMap);
			request.getRequestDispatcher("/locksdb/locksdb.jsp").forward(request, response);

		} catch (Exception e) {
			logger.error("procedure failed : {}", e.toString());
		}

	}

	private String isAbleConn(String ip) throws Exception {
		String cmd = "ps -ef | grep LocksMain | grep -v grep | wc -l";
		RmtShellExecutor exe = new RmtShellExecutor(ip,"locksuser","123456");
		String status = exe.exec(cmd);
		return status.trim();
	}

}
