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

public class Task4 {
	private static MyGenreDetector detector = new MyGenreDetector();
	private static final int GENRES_COUNT = Genre.values().length;
	
	public static void main(String[] args) throws Exception {
		
        if (args.length < 1) {
            System.err.println("Usage: Task4 <file>");
            System.exit(1);
        }
        
        SparkConf sparkConf = new SparkConf().setAppName("Task4");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);
        
        JavaPairRDD<Integer, float[]> songs = lines.mapToPair(
        		new PairFunction<String, Integer, float[]>() {
            @Override
            public Tuple2<Integer, float[]> call(String song) {
            	
				HashSet<SongProperty> prop = new HashSet<SongProperty>();
				prop.add(SongProperty.ARTIST_TERMS);
				prop.add(SongProperty.ARTIST_TERMS_FREQ);
				HashMap<SongProperty, String> val = InputParser.getSongProperties(song, prop);
				String artist_terms = val.get(SongProperty.ARTIST_TERMS);
				String artist_terms_freq = val.get(SongProperty.ARTIST_TERMS_FREQ);
				
				String[] terms = InputParser.stringToArrayString(artist_terms);
				float[] importances = InputParser.stringToArrayFloat(artist_terms_freq);
				Tuple2<Genre, float[]> tuple = detector.detectGenreCrossover(terms, importances);
				Genre genre = tuple._1();

                return new Tuple2<Integer, float[]>(genre.ordinal(), tuple._2());
            }
        });
        
        JavaPairRDD<Integer, float[]> sums = songs.reduceByKey(
        		new Function2<float[], float[], float[]>() {
            @Override
            public float[] call(float[] s1, float[] s2) {
            	
            	float[] res = new float[GENRES_COUNT];
            	
            	for (int i = 0; i < GENRES_COUNT; i++) {
            		res[i] = s1[i] + s2[i];
            	}

            	return res;
            }
        });
        
        List<Tuple2<Integer, float[]>> output = sums.collect();
		for (Tuple2<?,?> tuple : output) {
			Integer val = (Integer)tuple._1();
			float[] arr = (float[])tuple._2();
			System.out.println(Genre.values()[val] + " " + val + " " + arrayFloatToString(arr));
		}
        
		ctx.stop();
	}
	
	private static String arrayFloatToString(float[] arr) {
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < GENRES_COUNT; i++) {
			if (i != 0) {
				str.append(' ');
			}
			str.append(arr[i]);
		}
		return str.toString();
	}
}
