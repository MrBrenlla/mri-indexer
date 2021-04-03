package es.udc.fic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

class Similarity implements Comparable<Object>{
	private String term;
    private double nota;
    
    public String getTerm() {
		return term;
	}
    
	public double getNota() {
		return nota;
	}

    public Similarity(String t, double n) {
    	term=t;
    	nota=n;
    }
    
    @Override
    public int compareTo(Object s) {
        double compare=((Similarity)s).getNota();
        if (this.nota==compare) return 0;
        else if (this.nota<compare) return 1;
        else return -1;

        /* For Descending order do like this */
        //return compareage-this.studentage;
    }

    @Override
    public String toString() {
        return "termino=" + term + ", similaridade=" + nota;
    }
    
    
}


public class SimilarTerms {
	
	private static double cosineSimilarity(double[] vectorA, double[] vectorB) {
	    double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
	    for (int i = 0; i < vectorA.length; i++) {
	        dotProduct += vectorA[i] * vectorB[i];
	        normA += Math.pow(vectorA[i], 2);
	        normB += Math.pow(vectorB[i], 2);
	    }   
	    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	}
	
	private static double[] bin(String t, String f, IndexReader r) throws IOException {
		double[] v= new double[r.numDocs()];
		for (int i =0; i<r.numDocs();i++) {
			double aux;
			TermsEnum terms = r.getTermVector(i,f).iterator();			
			BytesRef term=terms.next();
			while(term!=null && !term.utf8ToString().equals(t)) term=terms.next();
			if (term==null) aux=0;
			else aux= terms.docFreq();
			v[i]=aux;
		}
		return v;
	}
	
	private static double[] tf(String t,String f, IndexReader r) throws IOException {
		double[] v= new double[r.numDocs()];
		ClassicSimilarity sim = new ClassicSimilarity();
		for (int i =0; i<r.numDocs();i++) {
			double aux;
			TermsEnum terms = r.getTermVector(i,f).iterator();			
			BytesRef term=terms.next();
			while(term!=null && !term.utf8ToString().equals(t)) term=terms.next();
			if (term==null) aux=0;
			else aux= sim.tf(terms.totalTermFreq());
			v[i]=aux;
		}
		return v;
		
	}
	private static double[] tfxidf(String t,String f, IndexReader r) throws IOException {
		double[] v= new double[r.numDocs()];
		ClassicSimilarity sim = new ClassicSimilarity();
		for (int i =0; i<r.numDocs();i++) {
			double aux;
			TermsEnum terms = r.getTermVector(i,f).iterator();			
			BytesRef term=terms.next();
			while(term!=null && !term.utf8ToString().equals(t)) term=terms.next();
			if (term==null) aux=0;
			else { 
				Term auxT = new Term(f,t);
				aux= sim.tf(terms.totalTermFreq())*sim.idf(r.docFreq(auxT), r.numDocs());
			}
			v[i]=aux;
		}
		return v;
	}
	
	
	public static void main(final String[] args) {

		String usage = "java es.udc.fic.WriteIndex" + " [-index INDEX_PATH] [-field FIELD] [-term TERM] [-top NUM] [-rep bin|tf|tfxidf]\n\n";
		
		Properties p = new Properties();
		try {
			p.load(Files.newInputStream(Path.of("./src/main/resources/config.properties")));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		String indexPath = null;
		String field = null;
		String content = null;
		String top = null;
		String rep= null;
		int n;
		
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-field".equals(args[i])) {
				field = args[i + 1];
				i++;
			}else if ("-term".equals(args[i])) {
				content = args[i + 1];
				i++;
			}else if ("-top".equals(args[i])) {
				top = args[i + 1];
				i++;
			}else if ("-rep".equals(args[i])) {
				rep = args[i + 1];
				i++;
			}
		} 
		
		
		
		if((indexPath==null)||(field==null)||(top==null)||(content==null)) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}

		n=Integer.valueOf(top);
		
		
		if(rep==null) {
			System.out.println("Not -rep especified, using tfxidf as default.");
			rep="tfxidf";
		}else if((!rep.equals("bin"))&&(!rep.equals("tf"))&&(!rep.equals("tfxidf"))) {
			System.out.println("-rep had a wrong argument, using tfxidf as default.");
			rep="tfxidf";
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
			
			
					
			if(reader.getTermVectors(0).terms(field)==null) {
				System.err.println("Field "+field+" doesn't exist");
				System.exit(1);
			}
			
			double[] base;
			switch(rep) {
			case "bin" :base=bin(content,field,reader);
				break;
			case "tf" :base=tf(content,field,reader);
				break;
			default :base=tfxidf(content,field,reader);
				break;
			}
			
			ArrayList<Similarity> rank=new ArrayList<>();
			
			Terms terms = MultiTerms.getTerms(reader, field);
			try {
				TermsEnum enumeration = terms.iterator();
				BytesRef auxterm;
				double[] aux;
				while((auxterm=enumeration.next())!= null) if(!auxterm.utf8ToString().equals(content)) { 
					switch(rep) {
					case "bin" :aux=bin(auxterm.utf8ToString(),field,reader);
						break;
					case "tf" :aux=tf(auxterm.utf8ToString(),field,reader);
						break;
					default :aux=tfxidf(auxterm.utf8ToString(),field,reader);
						break;
					}
					rank.add(new Similarity(auxterm.utf8ToString(),cosineSimilarity(aux,base)));
				}
				
				//Da un warning por usar a clase Comparable, a causa de que non se comproba se "rank" é un List<Comparable> dentro da función sort, pero funiona sen problemas.
				Collections.sort(rank);
				
				for(int i=0; i<n && i<rank.size();i++) System.out.println(rank.get(i).toString());
				
			} catch (java.lang.NullPointerException e) {
			}
					 			
						
						
			//sort---------------------
		
			reader.close();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
