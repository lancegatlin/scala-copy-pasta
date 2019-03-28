package org.ldg.test

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source
import org.ldg._

trait SparkSessionTestHelper extends FileTestHelper with LazyLogging {
  def appName: String = this.getClass.getName
  def sparkCfg: Seq[(String,String)] = Seq.empty

  /**
    * Construct a SparkSession, pass to provided function, then close the session
    * even if an exception is thrown
    *
    * @param f function to pass SparkSession to
    * @tparam A return type
    * @return value returned by function
    */
  def withSparkSession[A](f: SparkSession => A) : A =
    withSparkSession()(f)

  /**
    * Construct a SparkSession, pass to provided function, then close the session
    * even if an exception is thrown
    *
    * @param extraCfg (key,value) configuration pairs to configure SparkSession
    * @param f function to pass SparkSession to
    * @tparam A return type
    * @return value returned by function
    */
  def withSparkSession[A](extraCfg: (String,String)*)(f: SparkSession => A) : A = {
    val _sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)

    (sparkCfg ++ extraCfg).foreach { case (k,v) =>
      _sparkSession.config(k,v)
    }
    logger.info("Creating SparkSession")
    val sparkSession = _sparkSession.getOrCreate()
    val retv = try {
      f(sparkSession)
    } finally {
      logger.info("Closing SparkSession")
      sparkSession.close()
    }
    retv
  }

  /**
    * Write a DataFrame to a CSV String
    * @param df
    * @return output CSV String
    */
  def writeCsvToString(df: DataFrame) : String = {
    withTmpDir { tmpdir =>
      val outputDirPath = tmpdir.toString + "/output"
      df
        .coalesce(1)
        .write.format("csv")
        .option("overwrite","true")
        .option("header","true")
        .save(outputDirPath)

      logger.info(s"Wrote CSV ${df.count()} records to $outputDirPath")

      val outputFile =
        ls(outputDirPath)
          // note: should only be one
          .find(_.getName.endsWith(".csv"))
          .getOrDie(s"Did not find expected output CSV at $outputDirPath")

      Source.fromFile(outputFile).mkString
    }
  }

  /**
    * Convert a CSV string to DataFrame, transform and write back to CSV String
    * @param csv
    * @param f
    * @param sparkSession
    * @return output CSV String
    */
  def xfrmCsv(
    csv: String
  )(
    f: DataFrame => DataFrame
  )(implicit
    sparkSession: SparkSession
  ) : String = {
    import sparkSession.sqlContext.implicits._
    writeCsvToString {
      val df =
        sparkSession.read
          .option("header",true)
          .csv(csv.lines.toIndexedSeq.toDS())
      f(df)
    }
  }

}
