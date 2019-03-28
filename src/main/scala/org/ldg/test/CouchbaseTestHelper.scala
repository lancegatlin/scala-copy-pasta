package org.ldg.test

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import com.couchbase.client.java.{Cluster, CouchbaseCluster}
import com.couchbase.client.java.bucket.BucketType
import com.couchbase.client.java.cluster._
import com.couchbase.client.java.env.{CouchbaseEnvironment, DefaultCouchbaseEnvironment}
import com.couchbase.client.java.query.N1qlQuery
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.JavaConverters._

object CouchbaseTestHelper {
  case class CouchbaseCfg(
    nodes: List[String],
    username: String,
    password: String
  )
  case class CouchbaseConnection(
    env: CouchbaseEnvironment,
    cluster: Cluster,
    clusterManager: ClusterManager
  )
}

trait CouchbaseTestHelper extends MiscTestHelper with LazyLogging {
  import CouchbaseTestHelper._

  def couchbaseCfg: CouchbaseCfg

  private val tempBucketNames = new AtomicReference[List[String]](Nil)
  private def isTempBucketNameRegistered(tempBucketName: String) : Boolean =
    tempBucketNames.get.contains(tempBucketName)
  private def registerTempBucketName(tempBucketName: String) : Unit = {
    tempBucketNames.accumulateAndGet(List(tempBucketName), new java.util.function.BinaryOperator[List[String]] {
      override def apply(t: List[String], u: List[String]): List[String] = {
        t ++ u
      }
    })
  }
  private val isInit = new AtomicBoolean(false)
  private lazy val connection : CouchbaseConnection = {
    openCouchbaseConnection(
      nodes = couchbaseCfg.nodes,
      username = couchbaseCfg.username,
      password = couchbaseCfg.password
    ).effect { _ =>
      isInit.set(true)
    }
  }

  def withCouchbase[A](f: CouchbaseConnection => A): A = {
    // note: this is proper way but couchbase is very slow if initialized per call
//    val connection = openCouchbaseConnection(
//      nodes = couchbaseCfg.nodes,
//      username = couchbaseCfg.username,
//      password = couchbaseCfg.password
//    )
//    try {
      f(connection)
//    } finally {
//      connection.cluster.disconnect()
//      connection.env.shutdown()
//    }
  }

  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run(): Unit = {
      tempBucketNames.get.foreach { tempBucketName =>
        if(connection.clusterManager.hasBucket(tempBucketName)) {
          connection.clusterManager.removeBucket(tempBucketName)
          connection.clusterManager.removeUser(AuthDomain.LOCAL, tempBucketName)
        }
      }
      if(isInit.get) {
        connection.cluster.disconnect()
        connection.env.shutdown()
      }
    }
  })

  def withTempBucket[A](tempBucketName: String)(f: => A) : A = {
    // note: important not to leave cluster connected since Spark couchbase connector
    // spins up its own
    withCouchbase { implicit connection =>
      if(isTempBucketNameRegistered(tempBucketName)) {
        truncateBucket(tempBucketName)
      } else {
        createBucket(tempBucketName)
        createBucketUser(tempBucketName)
        createBucketPrimaryIndex(tempBucketName)
        registerTempBucketName(tempBucketName)
      }
    }

//    try {
      f
//    } finally {
//    }
  }

  def openCouchbaseConnection(
    nodes: List[String],
    username: String,
    password: String
  ) : CouchbaseConnection = {
    logger.info(s"Connecting to couchbase: nodes=${nodes.mkString(",")} username=$username password=${maskPassword(password)}")
    val env = DefaultCouchbaseEnvironment.builder().dnsSrvEnabled(false).build()
    val cluster = CouchbaseCluster.create(env, nodes.asJava)
    val clusterManager = cluster.clusterManager(username, password)
    CouchbaseConnection(
      env = env,
      cluster = cluster,
      clusterManager = clusterManager
    )
  }

  def createBucketUser(bucketName: String)(implicit connection: CouchbaseConnection) : Unit = {
    // must add temp bucket user with same name as bucket with password that is used to open bucket
    logger.info(s"Creating bucket user: $bucketName")
    connection.clusterManager.upsertUser(
      AuthDomain.LOCAL,
      bucketName,
      UserSettings.build()
        .name(bucketName)
        .password(bucketName)
        .roles(List(
          new UserRole("bucket_admin",bucketName),
          new UserRole("bucket_full_access",bucketName)
        ).asJava)
    )
  }

  def createBucketPrimaryIndex(bucketName: String)(implicit connection: CouchbaseConnection) : Unit = {
    // must open bucket to run create primary index query against it
    // note: need more time since bucket is new
    val bucket = connection.cluster.openBucket(bucketName,bucketName,30, TimeUnit.SECONDS)
    try {
      logger.info(s"Creating bucket primary index: $bucketName")
      val result = bucket.query(N1qlQuery.simple(s"CREATE PRIMARY INDEX `IDX_$bucketName` ON `$bucketName`"))
      if(result.errors().asScala.nonEmpty) {
        throw new RuntimeException(s"Failed to create primary index for bucket $bucketName:" + result.errors.toString)
      }
    } finally {
      bucket.close()
    }
  }

  def createBucket(bucketName: String)(implicit connection: CouchbaseConnection) : Unit = {
    val bucketSettings = new DefaultBucketSettings.Builder()
      .`type`(BucketType.COUCHBASE)
      .name(bucketName)
      .quota(100)
      .build()

    logger.info(s"Creating bucket: $bucketName")
    connection.clusterManager.insertBucket(bucketSettings)
  }

  def truncateBucket(bucketName: String)(implicit connection: CouchbaseConnection) : Unit = {
    val bucket = connection.cluster.openBucket(bucketName)
    try {
      bucket.bucketManager().flush()
    } finally {
      bucket.close()
    }
  }
}
