import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;

public class PageRank {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> rddInput = sparkContext.textFile(Config.livejournal).cache();
        rddInput = rddInput.filter(x -> !x.startsWith("#"));

        JavaPairRDD<Integer, ArrayList<Integer>> graph = JavaPairRDD.fromJavaRDD(rddInput.map(x->{
            String[] parts = x.split("\t");
            return new Tuple2<Integer, Integer>(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]));
        })).groupByKey()
                .mapToPair(x -> new Tuple2<>(x._1(), Lists.newArrayList(x._2())))
                .cache();
        Double init_pagerank = 1 / (double) Config.nodes_count;
        Broadcast<Double> init_pageRank = sparkContext.broadcast(init_pagerank);
        JavaPairRDD<Integer, Double> pageRank = graph.flatMapToPair(x -> {
            ArrayList<Tuple2<Integer, Double>> temp_list = new ArrayList<>();
            temp_list.add(new Tuple2<>(x._1(), init_pageRank.value()));
            for(Integer node : x._2()){
                temp_list.add(new Tuple2<>(node, init_pageRank.value()));
            }
            return temp_list;
        }).reduceByKey((x, y) -> x);
        JavaPairRDD<Integer, Boolean> leafsNode = graph.flatMapToPair(x -> {
            ArrayList<Tuple2<Integer, Boolean>> temp_list = new ArrayList<>();
            temp_list.add(new Tuple2<>(x._1(), Boolean.FALSE));
            for(Integer node : x._2()){
                temp_list.add(new Tuple2<>(node, Boolean.TRUE));
            }
            return temp_list;
        }).reduceByKey((x, y) -> x && y).cache();

        Double leaf_pagerank = 0.0;
        for(int i=0; i < Config.ITERATIONS; i++) {
            leaf_pagerank = pageRank.join(leafsNode).filter(x -> x._2()._2()).map(x -> x._2()._1()).reduce((x, y) -> x + y);
            leaf_pagerank = leaf_pagerank / Config.nodes_count;
            Broadcast<Double> broadcastVar = sparkContext.broadcast(leaf_pagerank);
            pageRank = pageRank.join(graph).flatMapToPair(pair -> {
                Double temp_pagerank = 0.0;
                ArrayList<Tuple2<Integer, Double>> temp_list_pagerank = new ArrayList<>();
                temp_pagerank = pair._2()._1() / pair._2()._2().size();
                for(Integer x : pair._2()._2()){
                    temp_list_pagerank.add(new Tuple2<>(x, temp_pagerank));
                }
                return temp_list_pagerank;
            }).reduceByKey((x, y) -> x + y).mapValues(x -> x + broadcastVar.value());
        }


        pageRank.mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).saveAsTextFile(String.valueOf(args[0]));
    }
}
