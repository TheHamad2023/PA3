import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class PageRank {

    public static void main(String[] args) throws Exception {
        // check args
        if (args.length != 2) {
            System.err.println("Usage: PageRank <title_file> <links_file>");
            System.exit(1);
        }

        // grab files and set num of iterations
        String titleFile = args[0];
        String linksFile = args[1];
        int iterations = 25;

        // create spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("PageRank").master("local")
                .getOrCreate();

        // load titles and give them ID's
        JavaRDD<String> titlesRDD = spark.read().textFile(titleFile).javaRDD();
        JavaPairRDD<Long, String> titles = titlesRDD
                // assigns each line in the RDD an index
                .zipWithIndex()
                // convert line and id to (key, value) pair
                // ex: (1, article_name)
                .mapToPair(t -> new Tuple2<>(t._2 + 1, t._1))
                // cache for later use
                .cache();

        // load the links. this is the graph showing the vertices and their edges
        JavaRDD<String> linkLines = spark.read().textFile(linksFile).javaRDD();
        // formats the links file into Tuple2's.
        // ex: (1, [2,3,4,7,9])
        // key: source article, value: array of ID's of other articles linked in the source article
        JavaPairRDD<Long, List<Long>> links = linkLines
                .mapToPair(line -> {
                    String[] parts = line.split(":");
                    long from = Long.parseLong(parts[0].trim());

                    List<Long> outgoing = new ArrayList<>();
                    if (parts.length > 1) {
                        for (String dest : parts[1].trim().split(" ")) {
                            if (!dest.isEmpty()) {
                                outgoing.add(Long.parseLong(dest));
                            }
                        }
                    }

                    return new Tuple2<>(from, outgoing);
                })
                .cache();
        
        // Calculate 1/N, where N is number of pages
        Double initialRankValue = 1.0 / titles.count();
        
        // Populate RDD with key Long the page ID and and value Double the initialRankValue
        JavaPairRDD<Long, Double> initialRank = titles.keys().mapToPair(k -> new Tuple2<>(k, initialRankValue));
        
        //Join the links RDD with initial ranks RDD to produce RDD with key: ID long,
        // val: (rank Double and its outgoing links list<Long>)
        JavaPairRdd<Long, Tuple2<Double, List<Long>>> joinedRdd = initialRank.join(links);

        //Create new RDD key, val with the new propogated ranks
        joinedRdd.flatMapToPair(page -> {
            List<Long> outlinks = page._2._2;

            //Calculate the share of the current page's rank to propogate to the next
            //possible page by diving it over the propability of visiting said page
            //Review slides 18-21 Week 6B for this
            Double rankToPropogate = (Double) page._2._1 / outlinks.size();

            // ArrayList<Double> propogatedRanks = new ArrayList<>();

            // Create new List of key, vals where key is id of page that current is
            //linked to and val is the new prpogated rank
            List<Tuple2<Long, Double>> propogatedRanks = new ArrayList<>();

            // For each outgoing link propogate a  new rank
            for (Long id : outlinks) {
                propogatedRanks.add(new Tuple2<>(id, rankToPropogate))
            }

            //Still not complete, need a way to sum up these poropogated ranks
            //and save them as the new pagerank
        });

    }
}
