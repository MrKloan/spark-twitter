package fr.esgi.spark.twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import twitter4j.Status

final class Country(name: String, code: String, lang: String) {

  def isSentFrom(tweet: Status): Boolean = {
    val place = tweet.getPlace
    val user = tweet.getUser

    val isCountry = place != null && place.getCountry != null && place.getCountry.eq(this.name)
    val isCountryCode = place != null && place.getCountryCode != null && place.getCountryCode.eq(this.code)
    val isUserLocation = user != null && user.getLocation != null && (user.getLocation.contains(this.name) || user.getLocation.eq(this.code))

    isCountry || isCountryCode || isUserLocation
  }

  def isLang(tweet: Status): Boolean = {
    val isTweetLang = tweet.getLang != null && tweet.getLang.eq(this.lang)
    val isUserLang = tweet.getUser != null && tweet.getUser.getLang != null && tweet.getUser.getLang.eq(this.lang)

    isTweetLang || isUserLang
  }
}

object Application extends App {

  val sparkConf = new SparkConf()
    .setAppName("Twitter Application")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
    .setMaster("local[*]")

  val streamingContext = new StreamingContext(sparkConf, Minutes(1))
  val twitterStream = TwitterUtils.createStream(streamingContext, None /*, Array("#Spark")*/)

  val france = new Country("France", "FR", "fr")
  val usa = new Country("United States", "US", "en")

  val frTweets = twitterStream.filter(france.isSentFrom)
  val frRt = frTweets.filter(_.isRetweet).filter(_.getRetweetedStatus != null)
  val frenchRetweetingUs = frRt.filter(tweet => usa.isSentFrom(tweet.getRetweetedStatus))
  val frenchRetweetingEnglish = frRt.filter(tweet => usa.isLang(tweet.getRetweetedStatus))
  val frenchRetweetingFrench = frRt.filter(tweet => france.isLang(tweet.getRetweetedStatus))

  val usTweets = twitterStream.filter(usa.isSentFrom)
  val usRt = usTweets.filter(_.isRetweet).filter(_.getRetweetedStatus != null)
  val usRetweetingFr = usRt.filter(tweet => france.isSentFrom(tweet.getRetweetedStatus))
  val usRetweetingFrench = usRt.filter(tweet => france.isLang(tweet.getRetweetedStatus))

  frenchRetweetingFrench.count().map(count => count + " FR RT French").print()
  frenchRetweetingUs.count().map(count => count + " FR RT US").print()
  frenchRetweetingEnglish.count().map(count => count + " FR RT English").print()
  usRetweetingFr.count().map(count => count + " US RT FR").print()
  usRetweetingFrench.count().map(count => count + " US RT French").print()

  streamingContext.start()
  streamingContext.awaitTermination()
}
