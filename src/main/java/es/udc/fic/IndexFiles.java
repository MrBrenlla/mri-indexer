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
import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.nio.charset.Charset;
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
import java.util.Properties;
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
import org.apache.lucene.store.MMapDirectory;

public class IndexFiles {
	public static class WorkerThread implements Runnable {

		private final Path folder;
		private final IndexWriter w;
		private final boolean parcialIndex;
		private final String[] fileTypes;
		private final String[] lines;

		public WorkerThread(final Path folder, IndexWriter w, boolean p,String[] t,String[] l) {
			this.folder = folder;
			this.w=w;
			this.parcialIndex = p;
			this.fileTypes = t;
			this.lines=l;
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
				indexDocs(w, folder,fileTypes,lines);
				if(this.parcialIndex) w.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}


	public static void main(final String[] args) {

		String usage = "java org.apache.lucene.demo.IndexFiles" + " [-index INDEX_PATH] [-update] [-numThreads NUM_THREADS] [-openmode append|create|create_or_append] [-partialIndexes]\n\n";
		
		Properties p = new Properties();
		try {
			p.load(Files.newInputStream(Path.of("./src/main/resources/config.properties")));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String indexPath = "index";
		String docsPath = p.getProperty("docs");
		boolean create = true;
		boolean parcial = false;
		String openmode = null;
		String[] types = null;
		int threads=0;
		String[] lines= new String[2];
		lines[0]=p.getProperty("onlyTopLines");
		lines[1]=p.getProperty("onlyBottomLines");
		
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-update".equals(args[i])) {
				create = false;
			} else if ("-onlyFiles".equals(args[i])) {
				types = p.getProperty("onlyFiles").split(" ");
			} else if ("-partialIndexes".equals(args[i])) {
				parcial = true;
			} else if ("-openmode".equals(args[i])) {
				openmode = args[i + 1];
				i++;
			}else if ("-numThreads".equals(args[i])) {
				threads = Integer.valueOf(args[i+1]);
				i++;
			}
		} 

		if(indexPath.equals("index")) {
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

			if ((!create && openmode==null) || openmode.equals("create_or_append") ) {
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			} else if (create && openmode.equals("create")) {
				iwc.setOpenMode(OpenMode.CREATE);
			} else if ( openmode.equals("append")) {
				iwc.setOpenMode(OpenMode.APPEND);
			} else {
				System.out.println("openMode error: Correct formats are append, create(not compatible with -upgrade) or create_or_append. Running create_or_append as default" );
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
			}
			IndexWriter finalwriter = new IndexWriter(dir, iwc);
			IndexWriter writer = finalwriter;

		/*
		 * Create a ExecutorService (ThreadPool is a subclass of ExecutorService) with
		 * so many thread as cores in my machine. This can be tuned according to the
		 * resources needed by the threads.
		 */
		final int numCores = Runtime.getRuntime().availableProcessors();
		if(threads<1) threads=numCores;
		final ExecutorService executor = Executors.newFixedThreadPool(threads);

		/*
		 * We use Java 7 NIO.2 methods for input/output management. More info in:
		 * http://docs.oracle.com/javase/tutorial/essential/io/fileio.html
		 *
		 * We also use Java 7 try-with-resources syntax. More info in:
		 * https://docs.oracle.com/javase/tutorial/essential/exceptions/
		 * tryResourceClose.html
		 */
		int i = 0;
		String parcialPath;
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(docsPath))) {
			ArrayList<Path> list = new ArrayList<Path>();
			/* We process each subfolder in a new thread. */
			for (final Path path : directoryStream) {
				if (Files.isDirectory(path)) {
					if (parcial) {
							parcialPath= p.getProperty("partialIndexes")+"/p"+ Integer.toString(i);
							System.out.println("Parcial indexing "+path.toString()+" to directory '" + parcialPath + "'...");

							dir = FSDirectory.open(Paths.get(parcialPath));
							analyzer = new StandardAnalyzer();
							iwc = new IndexWriterConfig(analyzer);

							if ((!create && openmode==null) || openmode.equals("create_or_append") ) {
								iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
							} else if (create && openmode.equals("create")) {
								iwc.setOpenMode(OpenMode.CREATE);
							} else if ( openmode.equals("append")) {
								iwc.setOpenMode(OpenMode.APPEND);
							} else {
								System.out.println("openMode error: Correct formats are append, create(not compatible with -upgrade) or create_or_append. Running create_or_append as default" );
								iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
							}
							writer = new IndexWriter(dir, iwc);
							i++;
					}
					final Runnable worker = new WorkerThread(path,writer,parcial,types,lines);
					/*
					 * Send the thread to the ThreadPool. It will be processed eventually.
					 */
					executor.execute(worker);
				}else {
					list.add(path);
				}
			}
			if (parcial) {
				parcialPath= p.getProperty("partialIndexes")+"/p"+ Integer.toString(i);
				System.out.println("Parcial indexing remaining files to directory '" + parcialPath + "'...");

				dir = FSDirectory.open(Paths.get(parcialPath));
				analyzer = new StandardAnalyzer();
				iwc = new IndexWriterConfig(analyzer);

				if ((!create && openmode==null) || openmode.equals("create_or_append") ) {
					iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				} else if (create && openmode.equals("create")) {
					iwc.setOpenMode(OpenMode.CREATE);
				} else if ( openmode.equals("append")) {
					iwc.setOpenMode(OpenMode.APPEND);
				} else {
					System.out.println("openMode error: Correct formats are append, create(not compatible with -upgrade) or create_or_append. Running create_or_append as default" );
					iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				}
				writer = new IndexWriter(dir, iwc);
			}
				/* We process each subfolder in a new thread. */
				for (final Path path : list) indexDocs(writer,path,types,lines);
				if(parcial) writer.close();

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
		
		System.out.println("Finished all threads");
		
		if(parcial) {
			System.out.println("Merging parcial indexes");
			String auxPath1;
			String auxPath2;
			parcialPath= p.getProperty("partialIndexes")+"/p"+ Integer.toString(i-1);
			for(int n=1;n<i+1;n++) {
				parcialPath= p.getProperty("partialIndexes")+"/s"+ Integer.toString(n);
				if(n==1) auxPath1= p.getProperty("partialIndexes")+"/p"+ Integer.toString(n-1);
				else auxPath1= p.getProperty("partialIndexes")+"/s"+ Integer.toString(n-1);
				auxPath2= p.getProperty("partialIndexes")+"/p"+ Integer.toString(n);
				dir = FSDirectory.open(Paths.get(parcialPath));
				analyzer = new StandardAnalyzer();
				iwc = new IndexWriterConfig(analyzer);

				if ((!create && openmode==null) || openmode.equals("create_or_append") ) {
					iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				} else if (create && openmode.equals("create")) {
					iwc.setOpenMode(OpenMode.CREATE);
				} else if ( openmode.equals("append")) {
					iwc.setOpenMode(OpenMode.APPEND);
				} else {
					System.out.println("openMode error: Correct formats are append, create(not compatible with -upgrade) or create_or_append. Running create_or_append as default" );
					iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				}
				writer = new IndexWriter(dir, iwc);
				
				writer.addIndexes(new MMapDirectory(Path.of(auxPath1)),new MMapDirectory(Path.of(auxPath2)));
				writer.close();
			}
			finalwriter.addIndexes(new MMapDirectory(Path.of(parcialPath)));
		}
		
			finalwriter.close();

			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
	}		
		
		static void indexDocs(final IndexWriter writer, Path path, String[] types,String[] lines) throws IOException {
			if (Files.isDirectory(path)) {
				Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						try {
							if (in(file.toString(),types)) indexDoc(writer, file, attrs.lastModifiedTime().toMillis(),lines);
						} catch (IOException ignore) {
							// don't index files that can't be read.
						}
						return FileVisitResult.CONTINUE;
					}
				});
			} else {
				if (in(path.toString(),types)) indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis(),lines);
			}
		}

		/** Indexes a single document */
		static void indexDoc(IndexWriter writer, Path file, long lastModified,String[] lines) throws IOException {
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
				BufferedReader buf = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
				if(lines[0]==null)lines[0]="0";
				if(lines[1]==null)lines[1]="0";
				int top = Integer.valueOf(lines[0]);
				int bot = Integer.valueOf(lines[1]);
				if ((top<=0) && (bot<=0)) doc.add(new TextField("contents",buf));
				else {
					String aux;
					ArrayList<String> c = new ArrayList<String>();
					while ((aux=buf.readLine())!=null) c.add(aux);
					if ((bot+top)>=c.size()) doc.add(new TextField("contents",buf));
					else {
						int i;
						aux="";
						for(i=0;i<top;i++) aux += c.get(i)+"\n";
						for(i=bot;i>0;i--) aux += c.get(c.size()-i)+"\n";
						InputStream inputStream = new ByteArrayInputStream(aux.getBytes(Charset.forName("UTF-8")));
						buf = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
						doc.add(new TextField("contents",buf));
					}
				}
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
		
		private static boolean in(String s, String[] list) {
			if (list==null) return true;
			boolean result = false;
			for (String i : list) if(s.endsWith(i)) result=true;
			return result;
		}

	}