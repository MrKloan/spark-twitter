package fr.esgi.spark.twitter

import fr.esgi.spark.twitter.Application.frenchTweets
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}

object Application extends App {

  val sparkConf = new SparkConf()
    .setAppName("Twitter Application")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
    .setMaster("local[*]")

  val streamingContext = new StreamingContext(sparkConf, Minutes(1))
  val twitterStream = TwitterUtils.createStream(streamingContext, None /*, Array("#Spark")*/)

  val tweetsWithPlace = twitterStream.filter(_.getPlace != null)
  val frenchTweets = tweetsWithPlace.filter(_.getPlace.getCountryCode equals "FR")
  val usTweets = tweetsWithPlace.filter(_.getPlace.getCountryCode equals "US")

  val frenchRT = frenchTweets
    .filter(_.isRetweeted)
    .filter(_.getRetweetedStatus.getPlace != null)
    .filter(_.getRetweetedStatus.getPlace.getCountryCode equals "US")

  val usRT = usTweets
    .filter(_.isRetweeted)
    .filter(_.getRetweetedStatus.getPlace != null)
    .filter(_.getRetweetedStatus.getPlace.getCountryCode equals "FR")

  frenchRT.print()
  usRT.print()

  streamingContext.start()
  streamingContext.awaitTermination()
}
