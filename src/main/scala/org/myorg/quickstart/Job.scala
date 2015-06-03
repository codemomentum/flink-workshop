package org.myorg.quickstart

import org.apache.flink.api.scala._

/**
 * dataset: http://dataartisans.github.io/flink-training/exercises/mailData.html
 * exercise: http://dataartisans.github.io/flink-training/exercises/mailCount.html
 */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment


    //, String, String, String, String
    val mails = env.readCsvFile[(String, String)](
      "/Users/halit/github/quickstart/src/main/resources/processed",
      lineDelimiter = "##//##",
      fieldDelimiter = "#|#",
      includedFields = Array(1, 2)
    )

    mails.map { t => (t._1.substring(0, 7), t._2, 1) }.groupBy(0, 1).reduce { (g1, g2) => (g1._1, g1._2, g1._3 + g2._3) }.print()

    //mails.first(1).print()

  }
}
