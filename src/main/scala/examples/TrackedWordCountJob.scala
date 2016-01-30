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
  val StartsWithVowelRegex = """^[aeiou]""".r
  val DoesntStartWithVowelRegex = """^[^aeiou]""".r

  val pipe = TextLine(args("input"))
    .flatMap('line -> 'word) { ln: String => ln.toLowerCase.replaceAll("[^a-z0-9\\s]", "").split("\\s+") }
    .groupBy('word) { _.size('count) }

  val startsWithVowel = doFakeWork(pipe, StartsWithVowelRegex, 'swv, 'swv_count)

  val doesntStartWithVowel = doFakeWork(pipe, DoesntStartWithVowelRegex, 'word, 'count)

  // even more useless work
  doesntStartWithVowel.joinWithSmaller('word -> 'swv, startsWithVowel, joiner = new OuterJoin())
    .write(Tsv(args("output")))

  // do some busy work to add stages to the job
  def doFakeWork(pipe: Pipe, pattern: Regex, word: Symbol, count: Symbol): Pipe = {
    pipe
      .filter('word) { w: String => !(pattern findFirstIn w).isEmpty }
      .map('count -> 'count) { c: String => c.toLong }
      .groupAll { _.sortBy('count).take(20) }
      .rename(('word, 'count) -> (word, count))
  }
}
