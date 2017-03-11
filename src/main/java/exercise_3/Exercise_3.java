package exercise_3;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.BasicConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;
import twitter4j.Status;

public class Exercise_3 {

	public static void sentimentAnalysis(JavaDStream<Status> statuses, final SQLContext sqlctx) {

		JavaPairDStream<Long, String> tweets = statuses.mapToPair(new PairFunction<Status, Long, String>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Long, String> call(Status status) throws Exception {
				return new Tuple2<Long, String>(status.getId(), status.getText());
			}
		});
		
// tweets.print();
		
		JavaPairDStream<Long, String> filteredTweets = tweets
				.filter(new Function<Tuple2<Long,String>, Boolean>() 
					{
						private static final long serialVersionUID = 1L;
						public Boolean call(Tuple2<Long, String> v1) throws Exception {
							return LanguageDetector.isEnglish(v1._2().toString());					
					}
		});
		
		
// filteredTweets.print();

		JavaDStream<Tuple2<Long, String>> replacedTweets = filteredTweets.map(new Function<Tuple2<Long,String>, Tuple2<Long, String>>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Long, String> call(Tuple2<Long, String> v1) throws Exception {
				String text = v1._2();
				text = text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase();
		        return new Tuple2<Long, String>(v1._1(), text);
			}
		});
		
// replacedTweets.print();		

//	remove stop words
		
		replacedTweets = replacedTweets.map(new Function<Tuple2<Long,String>, Tuple2<Long, String>>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<Long, String> call(Tuple2<Long, String> v1) throws Exception {
				String text = v1._2();
				List<String> stopWords = StopWords.getWords();
				
				for (String word : stopWords)
		        {
					text = text.replaceAll("\\b" + word + "\\b", "");
		        }
		        return new Tuple2<Long, String>(v1._1(), text);
			}
		});
		
// replacedTweets.print();

			JavaPairDStream<Tuple2<Long, String>, Float> positiveTweets = replacedTweets.mapToPair(new PairFunction<Tuple2<Long,String>, Tuple2<Long, String>, Float>() {

				private static final long serialVersionUID = 1L;

				public Tuple2<Tuple2<Long, String>, Float> call(Tuple2<Long, String> t) throws Exception {
					String text = t._2();
					Set<String> positiveWords = PositiveWords.getWords();
					String[] words = text.split(" ");
					int nWords = words.length;
					int nPositiveWords = 0;
					
					for (String word : words)
				        {
				            if (positiveWords.contains(word))
				            	{
				                	nPositiveWords++;
				                	// System.out.println("Word: " + word + " Positive!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				            	}	
				        }
					
					return new Tuple2<Tuple2<Long,String>, Float>(new Tuple2<Long, String>(t._1(), t._2()), (float)nPositiveWords/nWords);
				}
				
			});
			
			positiveTweets = positiveTweets.filter(
					new Function <Tuple2<Tuple2<Long,String>, Float>, Boolean>() {
						private static final long serialVersionUID = 1L;
						public Boolean call(Tuple2<Tuple2<Long, String>, Float> arg0) throws Exception {
							boolean iszero=true;
							if (arg0._2==0) iszero=false;
							return iszero;
						}
						
					}
				);

// positiveTweets.print();

			JavaPairDStream<Tuple2<Long, String>, Float> negativeTweets = replacedTweets.mapToPair(new PairFunction<Tuple2<Long,String>, Tuple2<Long, String>, Float>() {

				private static final long serialVersionUID = 1L;

				public Tuple2<Tuple2<Long, String>, Float> call(Tuple2<Long, String> t) throws Exception {
					String text = t._2();
					Set<String> negativeWords = NegativeWords.getWords();
					String[] words = text.split(" ");
					int nWords = words.length;
					int nNegativeWords = 0;
					
					for (String word : words)
			        {
			            if (negativeWords.contains(word))
			            	{
			            		nNegativeWords++;
			            		// System.out.println("Word: " + word + " Negative!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			            	}

			        }
					
					return new Tuple2<Tuple2<Long,String>, Float>(new Tuple2<Long, String>(t._1(), t._2()), (float)nNegativeWords/nWords);
				}
			});
			
			negativeTweets = negativeTweets.filter(
					new Function <Tuple2<Tuple2<Long,String>, Float>, Boolean>() {
						private static final long serialVersionUID = 1L;
						public Boolean call(Tuple2<Tuple2<Long, String>, Float> arg0) throws Exception {
							boolean iszero=true;
							if (arg0._2==0) iszero=false;
							return iszero;
						}
						
					}
				);
			
// negativeTweets.print();
			
JavaPairDStream<Tuple2<Long, String>, Tuple2<Float, Float>> joined = positiveTweets.join(negativeTweets);

// joined.print();
 
	JavaDStream<Tuple4<Long, String, Float, Float>> scoredTweets = joined.map(new Function<Tuple2<Tuple2<Long,String>,Tuple2<Float,Float>>, Tuple4<Long, String, Float, Float>>() {
	
		private static final long serialVersionUID = 1L;

		public Tuple4<Long, String, Float, Float> call(Tuple2<Tuple2<Long, String>, Tuple2<Float, Float>> v1) throws Exception {
			return new Tuple4<Long, String, Float, Float>(v1._1()._1(), v1._1()._2(), v1._2()._1(), v1._2()._2());
		}
	});
	
// scoredTweets.print();
	
	JavaDStream<Tuple5<Long, String, Float, Float, String>> result = scoredTweets.map(new Function<Tuple4<Long,String,Float,Float>, Tuple5<Long, String, Float, Float, String>>() {
		private static final long serialVersionUID = 1L;
		public Tuple5<Long, String, Float, Float, String> call(Tuple4<Long, String, Float, Float> v1) throws Exception {
			String sentiment;
			if (v1._3()>v1._4())
				{
					sentiment = "positive";
				}
			else
				{
					if ((Math.abs(v1._3() - v1._4()) < 0.0001))
						{
							sentiment = "neutral";
							// System.out.println("Neutral!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
						}
					else
						{
							sentiment = "negative";
						}
				}
			return new Tuple5<Long, String, Float, Float, String>(v1._1(), v1._2(), v1._3(), v1._4(), sentiment);
		}
	});
	
result.print();	

	}


}
