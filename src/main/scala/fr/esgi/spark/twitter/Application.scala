package fr.esgi.spark.twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
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

final case class CountryStreams(
  source: Country,
  destination: Country,
  mentions: DStream[Status],
  sourceLangRT: DStream[Status],
  destinationCountryRT: DStream[Status],
  destinationLangRT: DStream[Status]
)

object Application extends App {

  def computeCountryStreams(twitterStream: ReceiverInputDStream[Status], source: Country, destination: Country): CountryStreams = {
    val sourceTweets = twitterStream.filter(source.isSentFrom)

    val mentions = sourceTweets.filter(_.getUserMentionEntities.nonEmpty)
    val sourceRT = sourceTweets.filter(_.isRetweet).filter(_.getRetweetedStatus != null)
    val sourceLangRT = sourceRT.filter(tweet => source.isLang(tweet.getRetweetedStatus))
    val destinationCountryRT = sourceRT.filter(tweet => destination.isSentFrom(tweet.getRetweetedStatus))
    val destinationLangRT = sourceRT.filter(tweet => destination.isLang(tweet.getRetweetedStatus))

    CountryStreams(source, destination, mentions, sourceLangRT, destinationCountryRT, destinationLangRT)
  }

  def printCountryStreams(countryStreams: CountryStreams): Unit = {
    countryStreams.mentions.count().map(count => countryStreams.source.name + " mentions: " + count).print()
    countryStreams.sourceLangRT.count().map(count => countryStreams.source.name + " RT of " + countryStreams.source.lang + " tweets: " + count).print()
    countryStreams.destinationCountryRT.count().map(count => countryStreams.source.name + " RT of " + countryStreams.destination.name + " tweets: " + count).print()
    countryStreams.destinationLangRT.count().map(count => countryStreams.source.name + " RT of " + countryStreams.destination.lang + " tweets: " + count).print()
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

  val franceStreams = computeCountryStreams(twitterStream, france, usa)
  val usaStreams = computeCountryStreams(twitterStream, usa, france)

  printCountryStreams(franceStreams)
  printCountryStreams(usaStreams)

  streamingContext.start()
  streamingContext.awaitTermination()
}
