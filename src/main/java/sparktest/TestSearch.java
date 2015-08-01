package sparktest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

/**
 * Created by root on 7/31/15.
 */
public class TestSearch implements Serializable {

    public TestSearch() {
        String logFile = "/opt/infra/spark-1.4.1-bin-hadoop2.3/README.md";
        SparkConf conf = new SparkConf().setAppName("Test1").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        JavaRDD<String> filterRdd = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("a");
            }
        });
        long numAs = filterRdd.count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return s.contains("b");
            }
        }).count();

        System.out.println("Lines With a: " + numAs + ", Lines with b: " + numBs);
    }

    public static void main(String[] args) throws Exception {
        new TestSearch();
    }

}
