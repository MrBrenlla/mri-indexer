package es.udc.fic;
import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;


class Cluster {

    public ArrayList<Similarity> similaritys;
    public Similarity centroid;
    public int id;

    //Creates a new Cluster
    public Cluster(int id) {
        this.id = id;
        this.similaritys = new ArrayList();
    }

    public ArrayList<Similarity> getPoints() {
        return similaritys;
    }

    public void addSimilarity(Similarity similarity) {
        similaritys.add(similarity);
    }

    public void setSimilaritys(ArrayList<Similarity> similaritys) {
        this.similaritys = similaritys;
    }

    public Similarity getCentroid() {
        return centroid;
    }

    public void setCentroid(Similarity centroid) {
        this.centroid = centroid;
    }

    public int getId() {
        return id;
    }

    public void clear() {
        similaritys.clear();
    }

    public void plotCluster() {
        System.out.println("[Cluster: " + id+"]");
        System.out.println("[Centroid: " + centroid + "]");
        System.out.println("[Similaritys: \n");
        for(int i = 0; i< similaritys.size(); i++){
            System.out.println(similaritys.get(i).toString());
        }
        System.out.println("]");
    }

}


class KMeans {

    private int NUM_CLUSTERS;

    private ArrayList<Similarity> similaritys;
    private ArrayList<Cluster> clusters;

    public KMeans(int numClusters) {
        this.similaritys = new ArrayList();
        this.clusters = new ArrayList();
        NUM_CLUSTERS = numClusters;
    }

    public void init(ArrayList<Similarity> similaritys) {
        //Set Similaritys
        this.similaritys = similaritys;
        //Create Clusters and set Random Centroids
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            Cluster cluster = new Cluster(i);
            Similarity centroid = new Similarity("centroid cluster: " + i, new Random().nextDouble());
            cluster.setCentroid(centroid);
            clusters.add(cluster);
        }

        //Print Initial state
        plotClusters();
    }

    private void plotClusters() {
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            Cluster c = clusters.get(i);
            c.plotCluster();
        }
    }

    private void clearClusters() {
        for(Cluster cluster : clusters) {
            cluster.clear();
        }
    }

    public void calculate() {
        boolean finish = false;
        int iteration = 0;

        // Add in new data, one at a time, recalculating centroids with each new one.
        while(!finish) {
            //Clear cluster state
            clearClusters();

            ArrayList<Similarity> lastCentroids = getCentroids();

            //Assign points to the closer cluster
            assignCluster();

            //Calculate new centroids.
            calculateCentroids();

            iteration++;

            ArrayList<Similarity> currentCentroids = getCentroids();

            //Calculates total distance between new and old Centroids
            double distance = 0;
            for(int i = 0; i <  lastCentroids.size(); i++) {
               distance = distance + Math.abs(lastCentroids.get(i).getNota() - currentCentroids.get(i).getNota());
            }
            System.out.println("#################");
            System.out.println("Iteration: " + iteration);
            System.out.println("Centroid distances: " + distance);
            plotClusters();

            if(distance == 0) {
                finish = true;
            }
        }
    }

    private ArrayList<Similarity> getCentroids() {
        ArrayList<Similarity> centroids = new ArrayList(NUM_CLUSTERS);
        for(Cluster cluster : clusters) {
            Similarity aux = cluster.getCentroid();
            Similarity similarity = new Similarity(aux.getTerm(),aux.getNota());
            centroids.add(similarity);
        }
        return centroids;
    }

    private void assignCluster() {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;

        for(Similarity similarity : similaritys) {
            min = max;
            for(int i = 0; i < NUM_CLUSTERS; i++) {
                Cluster c = clusters.get(i);
                distance = Math.abs(c.getCentroid().getNota() - similarity.getNota());
                if(distance < min){
                    min = distance;
                    cluster = i;
                }
            }
            //point.setCluster(cluster);
            clusters.get(cluster).addSimilarity(similarity);
        }
    }

    private void calculateCentroids() {
        for(Cluster cluster : clusters) {
            double cont = 0;
            ArrayList<Similarity> list = cluster.getPoints();
            int n_similaritys = list.size();

            for(Similarity similarity : list) {
               cont = cont + similarity.getNota();
            }

            Similarity centroid = cluster.getCentroid();
            if(n_similaritys > 0) {
                double newCont = cont / n_similaritys;
                centroid.setNota(newCont);
            }
        }
    }
}

public class TermsClusters {

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

    public static void main(String[] args) {
        String usage = "java es.udc.fic.TermsClusters" + " [-index INDEX_PATH] [-field FIELD] [-term TERM] [-top NUM] "
                                                       + " [-rep bin|tf|tfxidf] [-k NUM_OF_CLUSTERS]\n\n";

        Properties p = new Properties();
        try {
            p.load(Files.newInputStream(Path.of("./src/main/resources/config.properties")));
        } catch (
                IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        String indexPath = null;
        String field = null;
        String content = null;
        String top = null;
        String rep= null;
        String kString= null;

        int n;
        int k;

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
            }else if ("-k".equals(args[i])) {
                kString = args[i + 1];
                i++;
            }
        }


        if((indexPath==null)||(field==null)||(top==null)||(content==null)||(kString==null)) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        n=Integer.valueOf(top);
        k=Integer.valueOf(kString);

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
                KMeans kMeansAlgorith;
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

                kMeansAlgorith = new KMeans(k);
                kMeansAlgorith.init(rank);
                kMeansAlgorith.calculate();

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
