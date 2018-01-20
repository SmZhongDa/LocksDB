package com.fiberhome.locksdb.loader;

import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriter implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(FileWriter.class);
	private final LinkedBlockingQueue<String> errorData;

	public FileWriter(LinkedBlockingQueue<String> errorData) {
		this.errorData = errorData;
	}

	private void write() throws InterruptedException {
		String line;
		while ((line = errorData.take()) != null) {
			logger.error(line);
		}
	}

	private void write0() {
		try {
			write();
		} catch (InterruptedException e) {
			write0();
		}

	}

	@Override
	public void run() {
		write0();
	}

}
