package com.fiberhome.locksdb.jetty;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fiberhome.locksdb.util.ConfigLoader;


import ch.ethz.ssh2.Session;

public class JettyStart {
	private Logger logger = LoggerFactory.getLogger(JettyStart.class);
	public static HashMap<String,Session> initMap = new HashMap<String,Session>(30);
	private int port = 23457;
	public static ConfigLoader rc;
	
	public JettyStart(ConfigLoader rc){
		JettyStart.rc = rc;
	}
	
	public JettyStart(ConfigLoader rc,int port){
		this(rc);
		this.port = port;
	}
	
	public  void start() throws Exception{
		JettyTolls.init(initMap);
		
		final Server server = new Server(this.port);
		final ContextHandlerCollection handler = new ContextHandlerCollection();


		final WebAppContext webapp = new WebAppContext();
		webapp.setContextPath("/");
		logger.info(rc.getJettyPath());
		webapp.setDefaultsDescriptor(rc.getJettyPath() + "/test/webdefault.xml");
		webapp.setResourceBase(rc.getJettyPath() + "/test/WebRoot/");
		webapp.setDescriptor(rc.getJettyPath() + "/test/WebRoot/WEB-INF/web.xml");
		ExecutorService service = Executors.newFixedThreadPool(1);
		
		service.execute(new Runnable() {
			@Override
			public void run() {
				try {
					handler.addHandler(webapp);
					server.setHandler(handler);
					server.start();
					server.join();
				} catch (Exception e) {
					logger.error("jettyStart procedure failed : {}", e.toString());
				}
			}
		});
		
		logger.info("jetty start successful...");
		ScheduledExecutorService ser = Executors.newScheduledThreadPool(1);
		logger.info("init jetty data : [ delay : {} seconds , interval : {} seconds ]", 60, 60);
		ser.scheduleWithFixedDelay(new DataInduction(),  60, 60, TimeUnit.SECONDS);
		
	}
}