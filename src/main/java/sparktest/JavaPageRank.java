
package sparktest;

import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.regex.Pattern;

public final class JavaPageRank {
  public static final String filename = "web-Google.txt";
  public static final int iter = 1;
  private static final Pattern SPACES = Pattern.compile("\\s+");

  private static class Sum implements Function2<Double, Double, Double> {
    @Override
    public Double call(Double a, Double b) {
      return a + b;
    }
  }

  public static void main(String[] args) throws Exception {
    SparkConf sparkConf = new SparkConf().setAppName("JavaPageRank").setMaster("local[*]");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    // Loads in input file. It should be in format of:
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     URL         neighbor URL
    //     ...
    JavaRDD<String> lines = ctx.textFile(filename, 1);

    // Loads all URLs from input file and initialize their neighbors.
    JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        String[] parts = SPACES.split(s);
        return new Tuple2<String, String>(parts[0], parts[1]);
      }
    }).distinct().groupByKey().cache();
    System.out.println("link count:" + links.count());

    JavaPairRDD<String, Integer> linkCounts = links.mapValues(new Function<Iterable<String>, Integer>() {
      @Override
      public Integer call(Iterable<String> strings) throws Exception {
        return Iterables.size(strings);
      }
    });
    JavaPairRDD<Integer, String> countLinks = linkCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
      @Override
      public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        return stringIntegerTuple2.swap();
      }
    });
    List<Tuple2<Integer, String>> top10Links = countLinks.sortByKey(false).take(10);
    for (Tuple2<?,?> tuple : top10Links) {
        System.out.println(tuple._2() + " link 갯수: " + tuple._1());
    }

    // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
      @Override
      public Double call(Iterable<String> rs) {
        return 1.0;
      }
    });

    JavaRDD<Tuple2<Iterable<String>, Double>> values = links.join(ranks).values();

    // Calculates URL contributions to the rank of other URLs.
    JavaPairRDD<String, Double> contribs = links.join(ranks).values()
            .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
              @Override
              public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> s) {
                int urlCount = Iterables.size(s._1());
                List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
                for (String n : s._1()) {
                  results.add(new Tuple2<String, Double>(n, s._2() / urlCount));
                }
                return results;
        }
    });

    // Re-calculates URL ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
      @Override
      public Double call(Double sum) {
        return 0.15 + sum * 0.85;
      }
    });


    // Collects all URL ranks and dump them to console.
    List<Tuple2<String, Double>> output = ranks.take(10);//ranks.collect();
    System.out.println("node count:" + output.size());
    for (Tuple2<?,?> tuple : output) {
        System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
    }

    // 평균

    ctx.stop();
  }
}
