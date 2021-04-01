package es.udc.fic;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class BestTerms {
    public static void main(final String[] args) {
        String usage = "java es.udc.fic.BestTerms" + " [-index INDEX_PATH] [-docID NUMBER_OF_DOCUMENT] " +
                       "[-field FIELD] [-field FIELD] [-top NUMBER_OF_TOP_TERMS] [-order ORDER[tf, df or tfxidf]] " +
                       "[-outpufile OUTPUT_FILE [OPTIONAL]]\n\n";

        Properties p = new Properties();
        try {
            p.load(Files.newInputStream(Path.of("./src/main/resources/config.properties")));
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        String indexPath = null;
        int docID = -1;
        String field = null;
        int top = -1;
        String order = null;
        String outputfile = null;


        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                i++;
            } else if ("-docID".equals(args[i])) {
                docID = Integer.parseInt(args[i + 1]);
                i++;
            } else if ("-field".equals(args[i])) {
                field = args[i + 1];
                i++;
            } else if ("-top".equals(args[i])) {
                top = Integer.parseInt(args[i + 1]);
                i++;
            } else if ("-order".equals(args[i])) {
                order = args[i + 1];
                i++;
            } else if ("-outputfile".equals(args[i])) {
                outputfile = args[i + 1];
                i++;
            }

        }
        if(indexPath==null || docID==-1 || field==null || top==-1 || order==null) {
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
            try {
                TermsEnum enumeration = reader.getTermVector(0,"path").iterator();
                HashMap<String, Float> tfmap = new HashMap<>();
                HashMap<String, Float> idfmap = new HashMap<>();
                HashMap<String, Float> tfxidfmap = new HashMap<>();

                BytesRef term;
                HashMap<String, Float> sortedMap = null;
                float idf;
                float tf;
                float ScoreTfxIdf;
                TFIDFSimilarity tfidfSIM = new ClassicSimilarity();
                Iterator<Map.Entry<String, Float>> it = null;

                while((term=enumeration.next())!= null) {
                    System.out.println(new Term("path",term).text());

                    tf = tfidfSIM.tf(reader.docFreq(new Term("path",term)));
                    tfmap.put(term.utf8ToString(), tf);

                    idf = tfidfSIM.idf(enumeration.docFreq(), reader.numDocs());
                    idfmap.put(term.utf8ToString(), idf);

                    ScoreTfxIdf = tf*idf;
                    tfxidfmap.put(term.utf8ToString(), ScoreTfxIdf);

                    System.out.println("Termino: " + term.utf8ToString() + "\n Score: " + ScoreTfxIdf);
                    if (order.equals("tf")) {
                        sortedMap = tfmap.entrySet().stream()
                                .sorted(Map.Entry.<String, Float>comparingByValue().reversed())
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                        (e1, e2) -> e1, LinkedHashMap::new));
                    } else if (order.equals("idf")) {
                        sortedMap = idfmap.entrySet().stream()
                                .sorted(Map.Entry.<String, Float>comparingByValue().reversed())
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                        (e1, e2) -> e1, LinkedHashMap::new));
                    } else if (order.equals("tfxidf")) {
                        sortedMap = tfxidfmap.entrySet().stream()
                                .sorted(Map.Entry.<String, Float>comparingByValue().reversed())
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                        (e1, e2) -> e1, LinkedHashMap::new));

                    } else {
                        System.err.println("This order doesn't exists");
                        System.exit(1);
                    }

                }
                //System.out.println(unsortedMap);

                int i = 0;
                it = sortedMap.entrySet().iterator();
                Map.Entry<String, Float> entry;
                if (outputfile != null) {
                    FileWriter fichero = null;
                    PrintWriter pw = null;
                    try{
                        fichero = new FileWriter(outputfile);
                        pw = new PrintWriter(fichero);
                        pw.printf("%-15s %6s %12s %12s %12s%n","Term"," | ", "tf", "idf", "tfxidf");
                        while (it.hasNext() && i < top) {
                            entry = it.next();
                            if (order.equals("tf")) {
                                pw.printf(
                                        "%-15s %6s %12s %12s %12s%n",
                                        entry.getKey(),
                                        " | ",
                                        entry.getValue(),
                                        idfmap.get(entry.getKey()),
                                        tfxidfmap.get(entry.getKey())
                                );
                            } else if (order.equals("idf")) {
                                pw.printf(
                                        "%-15s %6s %12s %12s %12s%n",
                                        entry.getKey(),
                                        " | ",
                                        tfmap.get(entry.getKey()),
                                        entry.getValue(),
                                        tfxidfmap.get(entry.getKey())
                                );
                            } else if (order.equals("tfxidf")) {
                                pw.printf(
                                        "%-15s %6s %12s %12s %12s%n",
                                        entry.getKey(),
                                        " | ",
                                        tfmap.get(entry.getKey()),
                                        idfmap.get(entry.getKey()),
                                        entry.getValue()
                                );
                            }
                            i++;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            // Nuevamente aprovechamos el finally para
                            // asegurarnos que se cierra el fichero.
                            if (null != fichero)
                                fichero.close();
                        } catch (Exception e2) {
                            e2.printStackTrace();
                        }
                    }
                } else {
                    System.out.printf("%-15s %6s %12s %12s %12s%n","Term"," | ", "tf", "idf", "tfxidf");
                    while (it.hasNext() && i < top) {
                        entry = it.next();
                        if (order.equals("tf")) {
                            System.out.printf(
                                    "%-15s %6s %12s %12s %12s%n",
                                    entry.getKey(),
                                    " | ",
                                    entry.getValue(),
                                    idfmap.get(entry.getKey()),
                                    tfxidfmap.get(entry.getKey())
                            );
                        } else if (order.equals("idf")) {
                            System.out.printf(
                                    "%-15s %6s %12s %12s %12s%n",
                                    entry.getKey(),
                                    " | ",
                                    tfmap.get(entry.getKey()),
                                    entry.getValue(),
                                    tfxidfmap.get(entry.getKey())
                            );
                        } else if (order.equals("tfxidf")) {
                            System.out.printf(
                                    "%-15s %6s %12s %12s %12s%n",
                                    entry.getKey(),
                                    " | ",
                                    tfmap.get(entry.getKey()),
                                    idfmap.get(entry.getKey()),
                                    entry.getValue()
                            );
                        }
                        i++;
                    }
                }
                System.out.println(sortedMap);

            } catch (java.lang.NullPointerException e) {
                System.err.println(e);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
