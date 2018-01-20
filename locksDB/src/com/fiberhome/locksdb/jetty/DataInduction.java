package com.fiberhome.locksdb.jetty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.fiberhome.locksdb.util.Config;

public class DataInduction implements Runnable {
	private Logger logger = LoggerFactory.getLogger(DataInduction.class);
	
	private String filePath = JettyStart.rc.getJettyPath() + "/data/tmp";
	
	
	public static List<String> listTime = new ArrayList<String>();
	public static List<String> listCpu= new ArrayList<String>();
	public static List<String> listIo= new ArrayList<String>();
	public static List<String> listMem= new ArrayList<String>();
	public static List<String> listOver= new ArrayList<String>();
	public static List<String> listQuery= new ArrayList<String>();
	public static List<String> listLoder= new ArrayList<String>();
	
	public static String SystemInfo;
	public static String MemInfo;
	public static String CpuPhysical;
	public static String CpuThread;
	public static String CpuType;
	
	
	
	public void Induction(){
		logger.info("data Induction start...");
		Path file = Paths.get(filePath);
		listTime.clear();
		listIo.clear();
		listCpu.clear();
		listMem.clear();
		listOver.clear();
		listQuery.clear();
		listLoder.clear();
		try (InputStream in = Files.newInputStream(file); BufferedReader reader = new BufferedReader(new InputStreamReader(in, Config.DEFAULTCHARSET));) {
			String line = null;

			while ((line = reader.readLine()) != null) {
				String[] str = line.split("\t");
				if(str.length != 12){
					logger.info("main.sh do not has 12.");
				    continue;
				}
				logger.info(line);
				listTime.add(str[0]);
				listCpu.add(str[1]);
				listIo.add(str[2]);
				listMem.add(str[3]);
				listOver.add(str[4]);
				listQuery.add(str[5]);
				listLoder.add(str[6]);
				SystemInfo = str[7];
				MemInfo = str[8];
				CpuType = str[9];
				CpuPhysical = str[10];
				CpuThread= str[11];
			}
		} catch (IOException e) {
			logger.info(e.toString());
		}
	}
	

	@Override
	public void run() {
		Induction();
		
	}

}
