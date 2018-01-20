package com.fiberhome.locksdb.scheduler;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fiberhome.locksdb.index.Lucene;
import com.fiberhome.locksdb.index.LuceneTaxoReader;
import com.fiberhome.locksdb.index.ReaderHandler;
import com.fiberhome.locksdb.util.LocksUtil;

public class TaxoReaderReloader implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(TaxoReaderReloader.class);

	@Override
	public void run() {
		reload();
	}

	private void reload() {
		for (String s : Lucene.TAXOWRITERMAP.keySet()) {
			String[] strs = s.split("_", -1);
			List<String> list = LocksUtil.getDays();
			if (strs.length == 2 && list.contains(strs[1])) {
				try {
					long time = System.currentTimeMillis();
					TaxonomyReader reader = DirectoryTaxonomyReader.openIfChanged(Lucene.TAXOREADERMAP.get(s).reader);
					if (reader != null) {
						logger.debug("reloading taxo index reader : {}", s);
						LuceneTaxoReader taxoReader = Lucene.TAXOREADERMAP.get(s);
						ReaderHandler.TAXODEPRECATEDQUEUE.put(taxoReader);
						Lucene.TAXOREADERMAP.put(s, new LuceneTaxoReader(s, reader));
						logger.info("reloaded taxo index reader : {} , elapsed time : {}", s, System.currentTimeMillis() - time);
					}
				} catch (IOException | InterruptedException e) {
					logger.error(e.toString());
				}
			}
		}
	}
}
