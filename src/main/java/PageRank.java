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
    }
}
