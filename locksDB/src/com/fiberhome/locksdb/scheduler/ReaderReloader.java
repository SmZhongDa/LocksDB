package com.fiberhome.locksdb.scheduler;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.index.LuceneReader;
import com.fiberhome.locksdb.index.ReaderHandler;
import com.fiberhome.locksdb.util.LocksUtil;

public class ReaderReloader implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ReaderReloader.class);

	@Override
	public void run() {
		reload();
		LocksUtil.sequenceReset();
	}

	private void reload() {
		for (String s : Lucene.INDEXWRITERMAP.keySet()) {
			String[] strs = s.split("_", -1);
			List<String> list = LocksUtil.getDays();
			if (strs.length == 2 && list.contains(strs[1])) {
				try {
					long time = System.currentTimeMillis();
					DirectoryReader reader = DirectoryReader.openIfChanged(Lucene.INDEXREADERMAP.get(s).reader);
					if (reader != null) {
						logger.debug("reloading index reader : {}", s);
						LuceneReader luceneReader = Lucene.INDEXREADERMAP.get(s);
						ReaderHandler.DEPRECATEDQUEUE.put(luceneReader);
						Lucene.INDEXREADERMAP.put(s, new LuceneReader(s, reader));
						logger.info("reloaded index reader : {} , elapsed time : {}", s, System.currentTimeMillis() - time);
					}
				} catch (IOException | InterruptedException e) {
					logger.error(e.toString());
				}
			}
		}
	}

}
