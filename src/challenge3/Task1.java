package challenge3;

import util.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class Task1 {

	public static void main(String[] args) throws Exception {
	
        if (args.length < 1) {
            System.err.println("Usage: Task1 <file>");
            System.exit(1);
        }
        
        SparkConf sparkConf = new SparkConf().setAppName("Task1");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        
        JavaPairRDD<Integer, Tuple2<Integer, Float>> songs = lines.mapToPair(
        		new PairFunction<String, Integer, Tuple2<Integer, Float>>() {
            @Override
            public Tuple2<Integer, Tuple2<Integer, Float>> call(String song) {
				HashSet<SongProperty> prop = new HashSet<SongProperty>();
				prop.add(SongProperty.YEAR);
				prop.add(SongProperty.LOUDNESS);
				HashMap<SongProperty, String> val = InputParser.getSongProperties(song, prop);
				String year = val.get(SongProperty.YEAR);
				String loud = val.get(SongProperty.LOUDNESS);
                return new Tuple2<Integer, Tuple2<Integer, Float>>(Integer.parseInt(year), 
                		new Tuple2<Integer, Float>(1, Float.parseFloat(loud)));
            }
        });
        
        
        JavaPairRDD<Integer, Tuple2<Integer, Float>> sums = songs.reduceByKey(
        		new Function2<Tuple2<Integer, Float>, Tuple2<Integer, Float>, Tuple2<Integer, Float>>() {
            @Override
            public Tuple2<Integer, Float> call(Tuple2<Integer, Float> s1, Tuple2<Integer, Float> s2) {

                int count = s1._1() + s2._1();
            	float loud = s1._2() + s2._2();
                
                return new Tuple2<Integer, Float>(count, loud);
            }
        });
        
        List<Tuple2<Integer, Tuple2<Integer, Float>>> output = sums.collect();
		for (Tuple2<?,?> tuple : output) {
			Tuple2<Integer, Float> t2 = (Tuple2<Integer, Float>)tuple._2();
			System.out.println(tuple._1() + " " + t2._1() + " " + t2._2());
		}
        
		ctx.stop();
	}
}
