package org.ldg.test

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql._
import com.holdenkarau.spark.testing.{ColumnGenerator, DataframeGenerator, Column => SingleColumnGen}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.types.StructType
import org.scalacheck.rng.Seed
import org.scalacheck.{Arbitrary, Gen}
import org.ldg._

object SparkGenDfHelper {

  object Generators {
    // note: these need to be outside trait since Gen is lazy and closes
    // over functions below (which could cause test object to be serialized

    def genStrN(n: Int) : Gen[String] =
      Gen.listOfN(n, Gen.alphaChar).map(_.mkString)

    def genStrRngN(min:Int, max: Int) : Gen[String] =
      for {
        n <- Gen.choose(min,max)
        str <- genStrN(n)
      } yield str

    def genTimestamp(when: Instant) : Gen[Timestamp] =
      Gen.const(Timestamp.from(when))

    def genCurrency() : Gen[BigDecimal] =
      Gen.choose(1, 1000000).map(c => BigDecimal(c,2))
  }
}

trait SparkGenDfHelper extends LazyLogging {

  def mkGenDataFrame(
    schema: StructType,
    numOfPartitions: Int = 1,
    customColumnGenerators: Seq[ColumnGenerator] = Seq.empty
  )(implicit
    sparkSession: SparkSession
  ) : Gen[DataFrame] = {
    val arbitraryDataFrame : Arbitrary[DataFrame] =
      DataframeGenerator.arbitraryDataFrameWithCustomFields(
        sqlContext = sparkSession.sqlContext,
        schema = schema,
        minPartitions = numOfPartitions
      )(customColumnGenerators:_*)

    arbitraryDataFrame.arbitrary
  }

  def genDataFrame(
    schema: StructType,
    rowCount: Int,
    customColumnGenerators: Seq[(String,Gen[Any])] = Seq.empty,
    numOfPartitions: Int = 1,
    seed: Seed = Seed.random()
  )(implicit
    sparkSession: SparkSession
  ) : DataFrame = {
    val _customColumnGenerators =
      customColumnGenerators.map { case (colName, colGen) =>
        new SingleColumnGen(colName,colGen)
      }
    val gen = mkGenDataFrame(
      schema = schema,
      numOfPartitions = numOfPartitions,
      customColumnGenerators = _customColumnGenerators
    )
    gen(Gen.Parameters.default.withSize(rowCount), seed)
      .getOrDie(s"Failed to generate dataframe with $rowCount rows for scheam $schema")
  }
}
