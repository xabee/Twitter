package exercise_2;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;
import twitter4j.Status;

public class Exercise_2 {

	@SuppressWarnings("deprecation")
	public static void get10MostPopularHashtagsInLast5min(
		JavaDStream<Status> statuses) {

		//Section 4.2.1
		
		JavaDStream<String> words = statuses
				.flatMap(new FlatMapFunction<Status, String>() {
					private static final long serialVersionUID = 1L;
					public Iterable<String> call(Status status) {
				          return Arrays.asList(status.getText().split(" "));
				      }
				});

		JavaDStream<String> hashTags = words
				.filter(new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;
					public Boolean call(String word) {
							return word.startsWith("#");					
						}
				});
		
		// hashTags.print();
		
		//Section 4.2.2

		JavaPairDStream<String, Integer> tuples = hashTags
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String in) {
						return new Tuple2<String, Integer>(in, 1);
					}
				});
		
		JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {
						return i1+i2;
					}
				}, new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {
						return i1-i2;
					}
				}, 
				new Duration(60 * 5 * 1000), 
				new Duration(1 * 1000)
			);
		
		// counts.print();

		//Section 4.2.3	
		
		JavaPairDStream<Integer, String> swappedCounts = counts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {	
					private static final long serialVersionUID = 1L;
					public Tuple2<Integer, String> call(
							Tuple2<String, Integer> in) {
						return in.swap();
					}
				}
			);

		JavaPairDStream<Integer, String> sortedCounts = swappedCounts
				.transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
					private static final long serialVersionUID = 1L;
					public JavaPairRDD<Integer, String> call(
							JavaPairRDD<Integer, String> in) throws Exception {
						return in.sortByKey(false);
					}
				});

		sortedCounts
				.foreachRDD(new Function<JavaPairRDD<Integer, String>, Void>() {
					private static final long serialVersionUID = 1L;
					public Void call(JavaPairRDD<Integer, String> rdd) {
						String out = "\nTop 10 hashtags:\n";
						for (Tuple2<Integer, String> t: rdd.take(10)) {
					           out = out + t.toString() + "\n";
					         }
					    System.out.println(out);
						return null;
					}
				});
		sortedCounts.print(); 
	}
}
