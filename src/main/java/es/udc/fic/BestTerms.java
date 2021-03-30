package es.udc.fic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

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
        String docID = null;
        String field = null;
        String top = null;
        String order = null;
        String outputfile = null;


        for (int i = 0; i < args.length; i++) {
            if ("-index".equals(args[i])) {
                indexPath = args[i + 1];
                i++;
            } else if ("-docID".equals(args[i])) {
                docID = args[i + 1];
                i++;
            } else if ("-field".equals(args[i])) {
                field = args[i + 1];
                i++;
            } else if ("-top".equals(args[i])) {
                top = args[i + 1];
                i++;
            } else if ("-order".equals(args[i])) {
                order = args[i + 1];
                i++;
            } else if ("-outputfile".equals(args[i])) {
                outputfile = args[i + 1];
                i++;
            }

        }
        if(indexPath==null || docID==null || field==null || top==null || order==null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        final Path indexDir = Paths.get(indexPath);
        if (!Files.isReadable(indexDir)) {
            System.out.println("Index in '" + indexDir.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        try {

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
