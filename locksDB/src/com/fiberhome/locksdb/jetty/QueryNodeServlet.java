package com.fiberhome.locksdb.jetty;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alibaba.fastjson.JSON;
import com.fiberhome.locksdb.client.LocksClient;
import com.fiberhome.locksdb.client.LocksIterator;
import com.fiberhome.locksdb.client.Pair;
import com.fiberhome.locksdb.client.request.LocksRequest;
import com.fiberhome.locksdb.sql.SQLParser;

public class QueryNodeServlet  extends HttpServlet  {

	private static final long serialVersionUID = -7807974415748706958L;
	
	private Logger logger = LoggerFactory.getLogger(QueryNodeServlet.class);
	
	public QueryNodeServlet(){
		super();
	}
	
	public void doGet(HttpServletRequest request, HttpServletResponse response) {
		this.doPost(request, response);
	}
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) {
		LocksClient client = null;
		try {
			final ConcurrentHashMap<String,List<HashMap<String, String>>> map = new ConcurrentHashMap<String,List<HashMap<String, String>>>();
		    final ConcurrentHashMap<String,String> countMap = new ConcurrentHashMap<String,String>();
		    final ConcurrentHashMap<String,String> timeMap = new ConcurrentHashMap<String,String>();
			List<LocksClient> ipList = new ArrayList<LocksClient>();
			for(String ip : GetQueryNodeServlet.nodeList){
				String tmpIp = request.getParameter("ip" + ip);
				if(null == tmpIp)
					continue;
				client = new LocksClient(tmpIp);
				Thread.sleep(1000);
				ipList.add(client);
			}
			String sql = request.getParameter("sql");
			logger.info("[ sql : {}  , ip : {} ]", sql,ipList);

		    LocksRequest.QueryBuilder builder = SQLParser.parse(sql);
//		    builder.setLimit(100);
		    builder.setTimeout(5000);
		    final LocksRequest req = builder.build();
		    
		    ExecutorService service = Executors.newFixedThreadPool(ipList.size());
		    for(final LocksClient tmpClient : ipList){
				service.execute(new Runnable() {
					@Override
					public void run() {
						try {
							logger.info("start querying ip : " + tmpClient.ip);
							long query_start = System.currentTimeMillis();
					    	LocksIterator it = tmpClient.send(req);
						    int count = 0;
						    List<HashMap<String, String>> list = new ArrayList<HashMap<String, String>>();

						    while (it.hasNext()) {
						    	HashMap<String,String> tmpMap = new HashMap<String,String>();
						    	count++;
								for (Pair<String, String> p : it.next()) {
									tmpMap.put(p.key,p.value);
								}
								list.add(tmpMap);
						    }
						    tmpClient.close();
						    map.put(tmpClient.ip, list);
						    countMap.put(tmpClient.ip, String.valueOf(count));
							long query_end = System.currentTimeMillis();
							long cost_time = query_end - query_start;
							timeMap.put(tmpClient.ip, String.valueOf(cost_time));

						} catch (Exception e) {
							logger.error("locksdb server : {} -- query error", tmpClient.ip);
							logger.error(e.toString());
						}
					}
				});
		    }
		    
			service.shutdown();
			try {
				service.awaitTermination(req.getTimeout(), TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error(e.toString());
			}
			service.shutdownNow();

		    QueryInfo info = new QueryInfo();
		    info.setResultMap(map);
		    info.setResultCount(countMap);
		    info.setTimeMap(timeMap);
		    
			response.setContentType("text/HTML;charset=UTF-8");
			response.setCharacterEncoding("UTF-8");
			PrintWriter out = response.getWriter();
			String jsonStr = JSON.toJSONString(info);
			logger.debug("jsonStr:" + jsonStr);
			out.write(jsonStr);	
		    
		} catch (Exception e) {
			client.close();
			logger.error("GetQueryNodeServlet happen error.." + e.toString());
		}
	}

}
