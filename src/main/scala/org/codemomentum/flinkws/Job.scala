package org.codemomentum.flinkws

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

/**
 * dataset: http://dataartisans.github.io/flink-training/exercises/mailData.html
 * exercise: http://dataartisans.github.io/flink-training/exercises/mailCount.html
 */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    //exercise 1
    val mails = env.readCsvFile[(String, String)](
      "src/main/resources/processed",
      lineDelimiter = "##//##",
      fieldDelimiter = "#|#",
      includedFields = Array(1, 2)
    )

    val grouped = mails.map { t => (t._1.substring(0, 7), t._2, 1) }.groupBy(0, 1).reduce { (g1, g2) => (g1._1, g1._2, g1._3 + g2._3) }
    grouped.print()


    //exercise 2
    val mailsWithAddresses = env.readCsvFile[(String, String, String)](
      "src/main/resources/processed",
      lineDelimiter = "##//##",
      fieldDelimiter = "#|#",
      includedFields = Array(0, 2, 5)
    )

    val replyConnections = mailsWithAddresses.join(mailsWithAddresses).where(2).equalTo(0) { (l, r) => (l._2, r._2, 1) }.groupBy(0, 1).reduce { (g1, g2) => (g1._1, g1._2, g1._3 + g2._3) }.sortPartition(2,Order.ASCENDING)

    replyConnections.print()

    //exercise 3
    val mailBodies = env.readCsvFile[(String, String)](
      "src/main/resources/processed",
      lineDelimiter = "##//##",
      fieldDelimiter = "#|#",
      includedFields = Array(0, 4)
    )

    val STOP_WORDS: Array[String] = Array (
      "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
      "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with", "you",
      "have", "as", "can")

    val D_SIZE = mailBodies.count

    /*
     TF-IDF is defined in two parts:

     The term-frequency tf(t, d) is the frequency of a term t in a document d, i.e., tf('house', d1) is equal to three if document d1 contains the word 'house' three times.
     The inverted-document-frequency idf(t, D) is the inverted fraction of documents that contain a term t in a collection of documents D, i.e., idf('house', D) = 1.25 if four documents in a collection of five documents contain the word 'house' (5 / 4 = 1.25).
     The TF-IDF metric for a term t in document d of a collection D is computed as

     tf-idf(t, d, D) = tf(t, d) * idf(t, D).

     */

    mailBodies.first(1).print()




  }
}
