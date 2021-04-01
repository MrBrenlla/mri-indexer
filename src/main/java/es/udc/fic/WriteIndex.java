package es.udc.fic;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class WriteIndex {

	
	public static void main(final String[] args) {

		String usage = "java es.udc.fic.WriteIndex" + " [-index INDEX_PATH] [-outputFile OUTPUT_FILE]\n\n";
		
		Properties p = new Properties();
		try {
			p.load(Files.newInputStream(Path.of("./src/main/resources/config.properties")));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String indexPath = null;
		String outPath = null;
		
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-outputFile".equals(args[i])) {
				outPath = args[i + 1];
				i++;
			}
		} 
		if(indexPath==null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}
		if(outPath==null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}
		
		final Path indexDir = Paths.get(indexPath);
		if (!Files.isReadable(indexDir)) {
			System.out.println("Index in '" + indexDir.toAbsolutePath()
					+ "' does not exist or is not readable, please check the path");
			System.exit(1);
		}
		Directory dir;
		try {
			dir = FSDirectory.open(Paths.get(indexPath));
			DirectoryReader reader= DirectoryReader.open(dir);
			List<LeafReaderContext> list = reader.leaves();
			LeafReader[] leafs= new LeafReader[list.size()];
			for(int i=0; i<list.size();i++) leafs[i]=list.get(i).reader();
			FieldInfos fInfos = leafs[0].getFieldInfos();
			String[] fields = new String[fInfos.size()];
			
			
			for(int i=0; i<fInfos.size();i++) fields[i]=fInfos.fieldInfo(i).name;
			String aux="";
			List<String> auxL;
			for(int i=0; i<fields.length;i++) {
				aux += "\n"+fields[i]+":\n\n";
				auxL=new ArrayList<>();
				for(LeafReader leaf : leafs) {
					Terms terms = leaf.terms(fields[i]);
					try {
						TermsEnum enumeration = terms.iterator();
						BytesRef term;
						while((term=enumeration.next())!= null) if(!auxL.contains(term.utf8ToString()+"\n")) auxL.add(term.utf8ToString()+"\n");
					} catch (java.lang.NullPointerException e) {
					}
				}
				if (auxL.isEmpty()) aux+= "----No Terms For This Filed----\n";
				for(String s:auxL) aux+=s;
			}
			
			reader.close();
			
			FileWriter fichero = null;
	        PrintWriter pw = null;
	        try{
	            fichero = new FileWriter(outPath);
	            pw = new PrintWriter(fichero);
	            pw.println(aux);

	        } catch (Exception e) {
	            e.printStackTrace();
	        } finally {
	           try {
	           if (null != fichero)
	              fichero.close();
	           } catch (Exception e2) {
	              e2.printStackTrace();
	           }
	        }
			
			System.out.println(aux);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
