package org.ldg.test

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.io.Source
import org.ldg._

object SparkSessionTestHelper extends LazyLogging {

  val sparkSessionCache = new TSMap[SparkTestConfig,SparkSession]

  case class SparkTestConfig(
    name: String,
    config: Seq[(String,String)]
  )

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      sparkSessionCache.entrySet().asScala.foreach { entry =>
        val sparkTestCfg = entry.getKey
        val sparkSession = entry.getValue
        logger.info(s"Closing SparkSession for config ${sparkTestCfg.name}")
        sparkSession.close()
      }
    }
  })
}

trait SparkSessionTestHelper extends FileTestHelper with LazyLogging {
  import SparkSessionTestHelper._

  def sparkTestCfg: SparkTestConfig = SparkTestConfig("default", Seq.empty)

  /**
    * Construct a SparkSession, pass to provided function, then close the session
    * even if an exception is thrown
    *
    * @param f function to pass SparkSession to
    * @tparam A return type
    * @return value returned by function
    */
  def withSparkSession[A](f: SparkSession => A) : A = {
    val sparkSession =
      sparkSessionCache.getOrCompute(sparkTestCfg) { () =>
          val _sparkSession =
            SparkSession
              .builder()
              .master("local[*]")
              .appName("SparkTest")

          sparkTestCfg.config.foreach { case (k,v) =>
            _sparkSession.config(k,v)
          }
          logger.info(s"Creating SparkSession[${sparkTestCfg.name}]")
          _sparkSession.getOrCreate()
      }

    f(sparkSession)
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

  def csvToDF(
    csv: String,
    schema: StructType
  )(implicit
    sparkSession: SparkSession
  ) : DataFrame = {
    import sparkSession.sqlContext.implicits._

    val csvLines = csv.lines.toList
    val csvDS =
      sparkSession.sparkContext
        .parallelize(csvLines)
        .toDS

    sparkSession.read.schema(schema).option("header","true").csv(csvDS)
  }
}
