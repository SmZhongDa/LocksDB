package com.fiberhome.locksdb.index;

import java.nio.file.Path;

import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;

public class LuceneTaxoWriter {

	public final DirectoryTaxonomyWriter writer;
	public final Path path;

	public LuceneTaxoWriter(DirectoryTaxonomyWriter writer, Path path) {
		this.writer = writer;
		this.path = path;
	}

}
