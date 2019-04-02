package org.ldg.test

import java.sql.Connection

import com.typesafe.scalalogging.slf4j.LazyLogging
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import scala.collection.JavaConverters._
import org.ldg._

object JdbcTestHelper {

  case class JdbcCfg(
    name: String,
    jdbcUrl: String,
    username: String,
    password: String
  )

  def runSql(sql: String)(implicit connection: Connection): Unit =
    connection.prepareStatement(sql).executeUpdate()

  sealed trait DbInitState

  object DbInitState {
    case object NotInit extends DbInitState
    case class Init(asyncCleanUp: () => Unit) extends DbInitState
  }

  // note: global to track different data sources & initSql across multiple tests
  private val jdbcCfgToDataSource = new TSMap[JdbcCfg, HikariDataSource]()
  private val dbIsInit = new TSMap[String, DbInitState]()

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      dbIsInit.entrySet().asScala.foreach { entry =>
        entry.getValue match {
          case DbInitState.Init(asyncCleanUp) =>
            asyncCleanUp()
          case DbInitState.NotInit =>
        }
      }
      jdbcCfgToDataSource.entrySet().asScala.foreach { entry =>
        entry.getValue.close()
      }
    }
  })
}


trait JdbcTestHelper extends FileTestHelper with MiscTestHelper with LazyLogging {
  import JdbcTestHelper._

  def testName: String

  def jdbcCfg: JdbcCfg

  def mkConnection() : Connection = {
    val dataSource = jdbcCfgToDataSource.getOrCompute(jdbcCfg){ () =>
      val config = new HikariConfig
      config.setJdbcUrl(jdbcCfg.jdbcUrl)
      config.setUsername(jdbcCfg.username)
      config.setPassword(jdbcCfg.password)

      new HikariDataSource(config)
    }

    dataSource.getConnection()
  }

  def withJdbc[A](f: Connection => A) : A = {
    logger.info(s"Connecting to JDBC[${jdbcCfg.name}]: url=${jdbcCfg.jdbcUrl} username=${jdbcCfg.username} password=${maskPassword(jdbcCfg.password)}")
    val connection = mkConnection()
    try {
      f(connection)
    } finally {
      logger.info(s"Close JDBC[${jdbcCfg.name}]")
      connection.close()
    }
  }

  val initSql = getResourceAsString("/init.sql")
  val cleanupSql = getResourceAsString("/cleanup.sql")
  def initDb()(implicit connection: Connection) : Unit = {
    if(dbIsInit.getOrCompute(testName)(() => DbInitState.NotInit) == DbInitState.NotInit) {
      logger.info(s"InitDb[$testName]")
      runSql(initSql)
      dbIsInit.put(testName, DbInitState.Init({() =>
        withJdbc { implicit connection =>
          logger.info(s"CleanUpDb[$testName]")
          runSql(cleanupSql)
        }
      }))
    }
  }

  def withInitDb[A](f: Connection => A) : A = {
    withJdbc { implicit connection =>
      try {
        initDb()
        f(connection)
      } finally {
      }
    }
  }

  def sqlQuote(str: String) :  String =
    s""""$str""""

  def countTable(table: String)(implicit connection: Connection) : Long = {
    // note: need quotes for table names that include underscore for postgres
    val sql = s"SELECT COUNT(*) FROM ${sqlQuote(table)}"
    val result =
      connection
        .prepareStatement(sql)
        .executeQuery()
    if(result.next()) {
      result.getLong(1)
    } else {
      die(s"Expected count from SQL: $sql")
    }
  }

  // todo: table to CSV?
}
