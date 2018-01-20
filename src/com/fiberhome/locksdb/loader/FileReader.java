package com.fiberhome.locksdb.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.util.Config;
import com.fiberhome.locksdb.util.ConfigLoader;
import com.fiberhome.locksdb.util.LocksUtil;

public class FileReader implements Runnable {

	private Logger logger = LoggerFactory.getLogger(FileReader.class);
	public final HashMap<String, LinkedBlockingQueue<String>> data = new HashMap<String, LinkedBlockingQueue<String>>();
	private final HashMap<String, HashMap<Integer, String>> indexMap;
	private final Path path;
	private volatile boolean isDone = false;
	private volatile long counter;
	private volatile boolean shutdown = false;
	private boolean flag;
	private SimpleDateFormat sdf;
	private Integer interval;

	/**
	 * @return the shutdown
	 */
	public boolean isShutdown() {
		return shutdown;
	}

	/**
	 * @param shutdown
	 *            the shutdown to set
	 */
	public void shutdown() {
		this.shutdown = true;
	}

	public boolean isDone() {
		return isDone;
	}

	public long getCounter() {
		return counter;
	}

	public FileReader(HashMap<String, HashMap<Integer, String>> indexMap, Path path) {
		this.indexMap = indexMap;
		this.path = path;
		this.sdf = new SimpleDateFormat("yyyyMMdd");
		this.sdf.setTimeZone(TimeZone.getTimeZone("GMT+08:00"));
		this.interval = Integer.parseInt(ConfigLoader.CONFIGMAP.get("interval"));
		for (String key : indexMap.keySet()) {
			data.put(key, new LinkedBlockingQueue<String>(100000));
		}
	}

	private void readFile(Path file) {
		Path fileName = file.getFileName();
		if (fileName.toString().endsWith("bcp")) {
			String[] strs = fileName.toString().split("_", -1);
			if (strs.length >= 4) {
				try {
					long fileTime = Long.parseLong(strs[1]);
					long today = LocksUtil.stringToSeconds(sdf, LocksUtil.secondsToString(sdf, System.currentTimeMillis() / 1000));
					if (!(today + 86400 * 2 > fileTime && fileTime >= today - 86400 * (interval - 1))){
						logger.info("fileTime is overdue");
						logger.info("delete overdue file : {}", file);
						try {
							Files.delete(file);
						} catch (IOException ex) {
							logger.info(ex.toString());
						}
						return;
					}
				} catch (NumberFormatException | ParseException e) {
					logger.error(e.toString());
					logger.info("delete error file : {}", file);
					try {
						Files.delete(file);
					} catch (IOException ex) {
						logger.info(ex.toString());
					}
					return;
				}
				
				String protocol = strs[3];
				if (indexMap.containsKey(protocol)) {
					flag = false;
					logger.info("reading file : {}", file);
					try (InputStream in = Files.newInputStream(file); BufferedReader reader = new BufferedReader(new InputStreamReader(in, Config.DEFAULTCHARSET));) {
						String line = null;
						while ((line = reader.readLine()) != null) {
							try {
								data.get(protocol).put(line);
								++counter;
							} catch (InterruptedException e) {
								logger.info(e.toString());
							}
						}
						logger.info("delete file : {}", file);
						Files.delete(file);
					} catch (IOException e) {
						logger.info(e.toString());
					}
				}
			}
		}
	}

	@Override
	public void run() {
		try {
			for (;;) {
				flag = true;
				Files.walkFileTree(path, EnumSet.allOf(FileVisitOption.class), 1, new FileVisitor<Path>() {
					@Override
					public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
						if (shutdown)
							return FileVisitResult.TERMINATE;
						return FileVisitResult.CONTINUE;
					}

					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						if (shutdown)
							return FileVisitResult.TERMINATE;
						readFile(file);
						return FileVisitResult.CONTINUE;
					}

					@Override
					public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
						if (null != exc)
							logger.error("visit file failed : {}", file);
						if (shutdown)
							return FileVisitResult.TERMINATE;
						return FileVisitResult.CONTINUE;
					}

					@Override
					public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
						if (shutdown)
							return FileVisitResult.TERMINATE;
						return FileVisitResult.CONTINUE;
					}

				});
				if (flag) {
					try {
						String s = ConfigLoader.CONFIGMAP.get("sleep");
						if (!s.equals("0")) {
							logger.info("FileReader sleep {} seconds", s);
							TimeUnit.SECONDS.sleep(Integer.parseInt(s));
						}
					} catch (InterruptedException e) {
						logger.error(e.toString());
					}
				}
			}
		} catch (IOException e) {
			logger.error(e.toString());
		} finally {
			isDone = true;
		}
	}
}
