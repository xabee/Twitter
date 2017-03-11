package exercise_1;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;

import twitter4j.Status;

public class Exercise_1 {

	public static void displayAllTweets(JavaDStream<Status> tweets) {
		JavaDStream<String> statuses = tweets.map(
			new Function<Status, String>() {
				public String call(Status status) {
					return "Tweet: " + status.getUser().getName() + " -- " + status.getText(); 
				}
			}
		);
		statuses.print();
	}
	
	
}
