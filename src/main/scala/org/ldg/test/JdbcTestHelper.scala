package org.ldg.test

import java.sql.Connection

import com.typesafe.scalalogging.slf4j.LazyLogging
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.ldg._


object JdbcTestHelper {
  case class JdbcCfg(
    jdbcUrl: String,
    username: String,
    password: String
  )
}

trait JdbcTestHelper extends FileTestHelper with MiscTestHelper with LazyLogging {
  import JdbcTestHelper._

  def jdbcCfg: JdbcCfg

  lazy val dataSource =  {
    val config = new HikariConfig
    config.setJdbcUrl(jdbcCfg.jdbcUrl)
    config.setUsername(jdbcCfg.username)
    config.setPassword(jdbcCfg.password)

    new HikariDataSource(config)
  }

  def mkConnection() : Connection = dataSource.getConnection()

  def withJdbc[A](f: Connection => A) : A = {
    logger.info(s"Connecting to JDBC: ${jdbcCfg.jdbcUrl} username=${jdbcCfg.username} password=${maskPassword(jdbcCfg.password)}")
    val connection = mkConnection()
    try {
      f(connection)
    } finally {
      logger.info(s"Close JDBC connection [${jdbcCfg.jdbcUrl}]")
      connection.close()
    }
  }

  def runSql(sql: String)(implicit connection: Connection) : Unit =
    connection.prepareStatement(sql).executeUpdate()

  val initSql = getResourceAsString("/init.sql")
  def initDb()(implicit connection: Connection) : Unit = {
    logger.info("Init db")
    runSql(initSql)
  }

  val cleanupSql = getResourceAsString("/cleanup.sql")
  def cleanupDb()(implicit connection: Connection) : Unit = {
    logger.info("Clean up db")
    runSql(cleanupSql)
  }

  def withInitDb[A](f: Connection => A) : A = {
    withJdbc { implicit connection =>
      try {
        initDb()
        f(connection)
      } finally {
        cleanupDb()
      }
    }
  }

  /**
    * Wrap an identifier string in double quotes to ensure Postgres
    * treats identifier as case-sensitive (also required for identifiers with underscore)
    * @param identifier
    * @return
    */
  def psqlQuote(identifier: String) : String = {
    if(identifier.startsWith(""""""") == false) {
      s""""$identifier""""
    } else {
      identifier
    }
  }
  def countTable(table: String)(implicit connection: Connection) : Long = {
    // note: need quotes for table names that include underscore for postgres
    val sql = s"SELECT COUNT(*) FROM ${psqlQuote(table)}"
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
