package fr.esgi.spark.twitter

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

final case class Country(name: String, code: String, lang: String) {

  def isSentFrom(tweet: Status): Boolean = {
    val place = tweet.getPlace
    val user = tweet.getUser

    val isCountry = place != null && place.getCountry != null && place.getCountry.equals(this.name)
    val isCountryCode = place != null && place.getCountryCode != null && place.getCountryCode.equals(this.code)
    val isUserLocation = user != null && user.getLocation != null && (user.getLocation.contains(this.name) || user.getLocation.equals(this.code))

    isCountry || isCountryCode || isUserLocation
  }

  def isLang(tweet: Status): Boolean = {
    val isTweetLang = tweet.getLang != null && tweet.getLang.equals(this.lang)
    val isUserLang = tweet.getUser != null && tweet.getUser.getLang != null && tweet.getUser.getLang.equals(this.lang)

    isTweetLang || isUserLang
  }
}

final case class CountryStats(
  source: Country,
  destination: Country,
  mentions: LongAccumulator,
  sourceLangRT: LongAccumulator,
  destinationCountryRT: LongAccumulator,
  destinationLangRT: LongAccumulator
)

object Application extends App {

  def computeCountryStreams(sparkContext: SparkContext, twitterStream: ReceiverInputDStream[Status], source: Country, destination: Country): CountryStats = {
    val sourceTweets = twitterStream.filter(source.isSentFrom)
    val sourceRT = sourceTweets.filter(_.isRetweet).filter(_.getRetweetedStatus != null)

    val mentions = sparkContext.longAccumulator
    sourceTweets.filter(_.getUserMentionEntities.nonEmpty).count().foreachRDD(countRdd => countRdd.collect.foreach(mentions.add))

    val sourceLangRT = sparkContext.longAccumulator
    sourceRT.filter(tweet => source.isLang(tweet.getRetweetedStatus)).count().foreachRDD(countRdd => countRdd.collect.foreach(sourceLangRT.add))

    val destinationCountryRT = sparkContext.longAccumulator
    sourceRT.filter(tweet => destination.isSentFrom(tweet.getRetweetedStatus)).count().foreachRDD(countRdd => countRdd.collect.foreach(destinationCountryRT.add))

    val destinationLangRT = sparkContext.longAccumulator
    sourceRT.filter(tweet => destination.isLang(tweet.getRetweetedStatus)).count().foreachRDD(countRdd => countRdd.collect.foreach(destinationLangRT.add))

    CountryStats(source, destination, mentions, sourceLangRT, destinationCountryRT, destinationLangRT)
  }

  def printCountryStreams(countryStats: CountryStats): Unit = {
    println(s"[${countryStats.source.name}] Mentions: ${countryStats.mentions.value}")
    println(s"[${countryStats.source.name}] RT of '${countryStats.source.lang}' tweets: ${countryStats.sourceLangRT.value}")
    println(s"[${countryStats.source.name}] RT of tweets from ${countryStats.destination.name}: ${countryStats.destinationCountryRT.value}")
    println(s"[${countryStats.source.name}] RT of '${countryStats.destination.lang}' tweets: ${countryStats.destinationLangRT.value}")
  }

  val sparkConf = new SparkConf()
    .setAppName("Twitter Application")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
    .setMaster("local[*]")

  val streamingContext = new StreamingContext(sparkConf, Seconds(15))
  val twitterStream = TwitterUtils.createStream(streamingContext, None, this.args)

  val france = Country("France", "FR", "fr")
  val usa = Country("United States", "US", "en")

  val franceStats = computeCountryStreams(streamingContext.sparkContext, twitterStream, france, usa)
  val usaStats = computeCountryStreams(streamingContext.sparkContext, twitterStream, usa, france)

  printCountryStreams(franceStats)
  printCountryStreams(usaStats)

  streamingContext.start()
  streamingContext.awaitTermination()
}
