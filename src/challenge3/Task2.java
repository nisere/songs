package challenge3;

import util.*;

import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public final class Task2 {

	public static void main(String[] args) throws Exception {
	
        if (args.length < 1) {
            System.err.println("Usage: Task2 <file>");
            System.exit(1);
        }
        
        SparkConf sparkConf = new SparkConf().setAppName("Task2");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Float, Float>> songs = lines.mapToPair(
        		new PairFunction<String, Tuple2<Integer, Integer>, Tuple3<Integer, Float, Float>>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Float, Float>> call(String song) {
				HashSet<SongProperty> prop = new HashSet<SongProperty>();
				prop.add(SongProperty.YEAR);
				prop.add(SongProperty.LOUDNESS);
				prop.add(SongProperty.TEMPO);
				prop.add(SongProperty.MODE);
				HashMap<SongProperty, String> val = InputParser.getSongProperties(song, prop);
				String year = val.get(SongProperty.YEAR);
				String loud = val.get(SongProperty.LOUDNESS);
				String tempo = val.get(SongProperty.TEMPO);
				String mode = val.get(SongProperty.MODE);
                return new Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Float, Float>>(
                		new Tuple2<Integer, Integer>(Integer.parseInt(year), Integer.parseInt(mode)), 
                		new Tuple3<Integer, Float, Float>(1, Float.parseFloat(loud), Float.parseFloat(tempo)));
            }
        });
        
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple3<Integer, Float, Float>> sums = songs.reduceByKey(
        		new Function2<Tuple3<Integer, Float, Float>, Tuple3<Integer, Float, Float>,	Tuple3<Integer, Float, Float>>() {
            @Override
            public Tuple3<Integer, Float, Float> call(Tuple3<Integer, Float, Float> s1, 
            		Tuple3<Integer, Float, Float> s2) {

            	int count = s1._1() + s2._1();
            	float loud = s1._2() + s2._2();
                float tempo = s1._3() + s2._3();
                
                return new Tuple3<Integer, Float, Float>(count, loud, tempo);
            }
        });
        
        List<Tuple2<Tuple2<Integer, Integer>, Tuple3<Integer, Float, Float>>> output = sums.collect();
		for (Tuple2<?,?> tuple : output) {
			Tuple2<Integer, Integer> t1 = (Tuple2<Integer, Integer>)tuple._1();
			Tuple3<Integer, Float, Float> t2 = (Tuple3<Integer, Float, Float>)tuple._2();
			System.out.println(t1._2() + " " + t1._1() + " " + t2._1() + " " + t2._2() + " " + t2._3());
		}
        
		ctx.stop();
	}
}
