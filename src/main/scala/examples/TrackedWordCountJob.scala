package com.etsy.sahale.examples


import cascading.pipe.Pipe
import cascading.pipe.joiner.OuterJoin
import com.etsy.sahale._
import com.twitter.scalding._
import scala.util.matching.Regex


/**
 * Variation on Scalding word-count example that generates a
 * multi-step job run that looks nice in the tracker UI :)
 *
 * Usage:
 * bin/runjob com.etsy.sahale.examples.TrackedWordCountJob --input /hdfs/path/to/input/dir --output /hdfs/path/to/output/dir
 */
class TrackedWordCountJob(args : Args) extends TrackedJob(args) {
  val pipe = TextLine(args("input"))
    .flatMap('line -> 'word) { line : String => tokenize(line) }
    .groupBy('word) { _.size('count) }

  val startsWithVowel = doFakeWork(pipe, """^[aeiou]""".r).rename(('word, 'count) -> ('vowel, 'vowel_count))
  val startsWithConsonant = doFakeWork(pipe, """^[^aeiou]""".r)

  // even more useless work
  startsWithConsonant
    .joinWithSmaller('word -> 'vowel, startsWithVowel, joiner = new OuterJoin())
    .write(Tsv(args("output")))

  def doFakeWork(pipe: Pipe, pattern: Regex): Pipe = {
    pipe
      .filter('word) { w: String => w match {case pattern(_) => true ; case _ => false} }
      .map('count -> 'count) { c: String => c.toLong }
      .groupAll { _.sortBy('count).take(10) }
  }

  // Split a piece of text into individual words.
  def tokenize(text : String) : Array[String] = {
    // Lowercase each word and remove punctuation.
    text.toLowerCase.replaceAll("[^a-zA-Z0-9\\s]", "").split("\\s+")
  }
}
