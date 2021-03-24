package es.udc.fic;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class IndexFiles {

	/**
	 * This Runnable takes a folder and prints its path.
	 */
	public static class WorkerThread implements Runnable {

		private final Path folder;
		private final IndexWriter w;

		public WorkerThread(final Path folder, IndexWriter w) {
			this.folder = folder;
			this.w=w;
		}

		/**
		 * This is the work that the current thread will do when processed by the pool.
		 * In this case, it will only print some information.
		 */
		@Override
		public void run() {
			System.out.println(String.format("I am the thread '%s' and I am responsible for folder '%s'",
					Thread.currentThread().getName(), folder));
			try {
				indexDocs(w, folder);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void main(final String[] args) {

		String usage = "java org.apache.lucene.demo.IndexFiles" + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n"
				+ "This indexes the documents in DOCS_PATH, creating a Lucene index"
				+ "in INDEX_PATH that can be searched with SearchFiles";
		String indexPath = "index";
		String docsPath = null;
		boolean create = true;
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-docs".equals(args[i])) {
				docsPath = args[i + 1];
				i++;
			} else if ("-update".equals(args[i])) {
				create = false;
			}
		}

		if (docsPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		final Path docDir = Paths.get(docsPath);
		if (!Files.isReadable(docDir)) {
			System.out.println("Document directory '" + docDir.toAbsolutePath()
					+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}

		Date start = new Date();
		try {
			System.out.println("Indexing to directory '" + indexPath + "'...");

			Directory dir = FSDirectory.open(Paths.get(indexPath));
			Analyzer analyzer = new StandardAnalyzer();
			IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

			if (create) {
				// Create a new index in the directory, removing any
				// previously indexed documents:
				iwc.setOpenMode(OpenMode.CREATE);
			} else {
				// Add new documents to an existing index:
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			}
			IndexWriter writer = new IndexWriter(dir, iwc);

		/*
		 * Create a ExecutorService (ThreadPool is a subclass of ExecutorService) with
		 * so many thread as cores in my machine. This can be tuned according to the
		 * resources needed by the threads.
		 */
		final int numCores = Runtime.getRuntime().availableProcessors();
		final ExecutorService executor = Executors.newFixedThreadPool(numCores);

		/*
		 * We use Java 7 NIO.2 methods for input/output management. More info in:
		 * http://docs.oracle.com/javase/tutorial/essential/io/fileio.html
		 *
		 * We also use Java 7 try-with-resources syntax. More info in:
		 * https://docs.oracle.com/javase/tutorial/essential/exceptions/
		 * tryResourceClose.html
		 */
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(docsPath))) {
			ArrayList<Path> list = new ArrayList<Path>();
			/* We process each subfolder in a new thread. */
			for (final Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					final Runnable worker = new WorkerThread(path,writer);
					/*
					 * Send the thread to the ThreadPool. It will be processed eventually.
					 */
					executor.execute(worker);
				}else {
					list.add(path);
				}
			}
				/* We process each subfolder in a new thread. */
				for (final Path path : list) {
					if (!Files.isDirectory(path)) {
						indexDocs(writer,path);
					}	
				}

		} catch (final IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		/*
		 * Close the ThreadPool; no more jobs will be accepted, but all the previously
		 * submitted jobs will be processed.
		 */
		executor.shutdown();

		/* Wait up to 1 hour to finish all the previously submitted jobs */
		try {
			executor.awaitTermination(1, TimeUnit.HOURS);
		} catch (final InterruptedException e) {
			e.printStackTrace();
			System.exit(-2);
		}
			writer.close();

			Date end = new Date();
			System.out.println("Finished all threads");
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
	}		
		
		static void indexDocs(final IndexWriter writer, Path path) throws IOException {
			if (Files.isDirectory(path)) {
				Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						try {
							indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
						} catch (IOException ignore) {
							// don't index files that can't be read.
						}
						return FileVisitResult.CONTINUE;
					}
				});
			} else {
				indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
			}
		}

		/** Indexes a single document */
		static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
			try (InputStream stream = Files.newInputStream(file)) {
				// make a new, empty document
				Document doc = new Document();

				// Add the path of the file as a field named "path". Use a
				// field that is indexed (i.e. searchable), but don't tokenize
				// the field into separate words and don't index term frequency
				// or positional information:
				Field pathField = new StringField("path", file.toString(), Field.Store.YES);
				doc.add(pathField);
				Field hostname = new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES);
				doc.add(hostname);
				Field thread = new StringField("thread", Thread.currentThread().getName(), Field.Store.YES);
				doc.add(thread);
				Field sizeKB = new StringField("sizeKB", String.valueOf(file.toFile().length()/1000), Field.Store.YES);
				doc.add(sizeKB);
				
				FileTime ct = Files.readAttributes(file, BasicFileAttributes.class).creationTime();
				FileTime lat = Files.readAttributes(file, BasicFileAttributes.class).creationTime();
				FileTime lmt = Files.readAttributes(file, BasicFileAttributes.class).creationTime();
				Field creationTime = new StringField("creationTime",ct.toString(), Field.Store.YES);
				doc.add(creationTime);
				Field lastAccessTime = new StringField("lastAccessTime",lat.toString(), Field.Store.YES);
				doc.add(lastAccessTime);
				Field lastModifiedTime = new StringField("lastModifiedTime",lmt.toString(), Field.Store.YES);
				doc.add(lastModifiedTime);
				Field creationTimeLucene = new StringField("creationTimeLucene",DateTools.timeToString(ct.toMillis(), DateTools.Resolution.MILLISECOND), Field.Store.YES);
				doc.add(creationTimeLucene);
				Field lastAccessTimeLucene = new StringField("lastAccessTimeLucene",DateTools.timeToString(lat.toMillis(), DateTools.Resolution.MILLISECOND), Field.Store.YES);
				doc.add(lastAccessTimeLucene);
				Field lastModifiedTimeLucene = new StringField("lastModifiedTimeLucene",DateTools.timeToString(lmt.toMillis(), DateTools.Resolution.MILLISECOND), Field.Store.YES);
				doc.add(lastModifiedTimeLucene);

				// Add the last modified date of the file a field named "modified".
				// Use a LongPoint that is indexed (i.e. efficiently filterable with
				// PointRangeQuery). This indexes to milli-second resolution, which
				// is often too fine. You could instead create a number based on
				// year/month/day/hour/minutes/seconds, down the resolution you require.
				// For example the long value 2011021714 would mean
				// February 17, 2011, 2-3 PM.
				doc.add(new LongPoint("modified", lastModified));

				// Add the contents of the file to a field named "contents". Specify a Reader,
				// so that the text of the file is tokenized and indexed, but not stored.
				// Note that FileReader expects the file to be in UTF-8 encoding.
				// If that's not the case searching for special characters will fail.
				doc.add(new TextField("contents",
						new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

				if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
					// New index, so we just add the document (no old document can be there):
					System.out.println("adding " + file);
					writer.addDocument(doc);
				} else {
					// Existing index (an old copy of this document may have been indexed) so
					// we use updateDocument instead to replace the old one matching the exact
					// path, if present:
					System.out.println("updating " + file);
					writer.updateDocument(new Term("path", file.toString()), doc);
				}
			}
		}

	}