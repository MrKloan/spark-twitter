package fr.esgi.spark.twitter

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
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

  def initCountryStats(sparkContext: SparkContext, source: Country, destination: Country): CountryStats =
    CountryStats(source, destination, sparkContext.longAccumulator, sparkContext.longAccumulator, sparkContext.longAccumulator, sparkContext.longAccumulator)

  def computeMentions(stream: DStream[Status], countryStats: CountryStats): Unit = stream
    .filter(_.getUserMentionEntities.nonEmpty)
    .count()
    .foreachRDD(countRdd => countRdd.collect.foreach(count => {
      countryStats.mentions.add(count)
      println(s"[${countryStats.source.name}] Mentions: $count new; ${countryStats.mentions.value} since startup.")
    }))

  def computeSourceLangRT(stream: DStream[Status], countryStats: CountryStats): Unit = stream
    .filter(tweet => countryStats.source.isLang(tweet.getRetweetedStatus))
    .count()
    .foreachRDD(countRdd => countRdd.collect.foreach(count => {
      countryStats.sourceLangRT.add(count)
      println(s"[${countryStats.source.name}] RT of '${countryStats.source.lang}' tweets: $count new; ${countryStats.sourceLangRT.value} since startup.")
    }))

  def computeDestinationCountryRT(stream: DStream[Status], countryStats: CountryStats): Unit = stream
    .filter(tweet => countryStats.destination.isSentFrom(tweet.getRetweetedStatus))
    .count()
    .foreachRDD(countRdd => countRdd.collect.foreach(count => {
      countryStats.destinationCountryRT.add(count)
      println(s"[${countryStats.source.name}] RT of tweets from ${countryStats.destination.name}: $count new; ${countryStats.destinationCountryRT.value} since startup.")
    }))

  def computeDestinationLangRT(stream: DStream[Status], countryStats: CountryStats): Unit = stream
    .filter(tweet => countryStats.destination.isLang(tweet.getRetweetedStatus))
    .count()
    .foreachRDD(countRdd => countRdd.collect.foreach(count => {
      countryStats.destinationLangRT.add(count)
      println(s"[${countryStats.source.name}] RT of '${countryStats.destination.lang}' tweets: $count new; ${countryStats.destinationLangRT.value} since startup.")
    }))

  def computeCountryStreams(twitterStream: ReceiverInputDStream[Status], countryStats: CountryStats): Unit = {
    val sourceTweets = twitterStream.filter(countryStats.source.isSentFrom)
    val sourceRT = sourceTweets.filter(_.isRetweet).filter(_.getRetweetedStatus != null)

    computeMentions(sourceTweets, countryStats)
    computeSourceLangRT(sourceRT, countryStats)
    computeDestinationCountryRT(sourceRT, countryStats)
    computeDestinationLangRT(sourceRT, countryStats)
  }

  val sparkConf = new SparkConf()
    .setAppName("Twitter Application")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
    .setMaster("local[*]")

  val streamingContext = new StreamingContext(sparkConf, Minutes(1))
  val twitterStream = TwitterUtils.createStream(streamingContext, None, this.args)

  val france = Country("France", "FR", "fr")
  val usa = Country("United States", "US", "en")

  val franceStats = initCountryStats(streamingContext.sparkContext, france, usa)
  val usaStats = initCountryStats(streamingContext.sparkContext, usa, france)

  computeCountryStreams(twitterStream, franceStats)
  computeCountryStreams(twitterStream, usaStats)

  streamingContext.start()
  streamingContext.awaitTermination()
}
