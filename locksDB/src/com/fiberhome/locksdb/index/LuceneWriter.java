package com.fiberhome.locksdb.index;

import java.nio.file.Path;
import org.apache.lucene.index.IndexWriter;

public class LuceneWriter {

	public final IndexWriter writer;
	public final Path path;

	public LuceneWriter(IndexWriter writer, Path path) {
		this.writer = writer;
		this.path = path;
	}

}
