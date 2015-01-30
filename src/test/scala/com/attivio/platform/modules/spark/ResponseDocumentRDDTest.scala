package com.attivio.platform.modules.spark

import com.attivio.app.Attivio
import com.attivio.bus.PsbProperties
import com.attivio.messages.QueryRequest
import com.attivio.model.document.{AttivioDocument, ResponseDocument}
import com.attivio.model.query.phrase.TermRange
import com.attivio.sdk.client.{DefaultAieClientFactory, ConnectorClient}
import com.attivio.test.EsbTestUtils
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Assert._
import org.junit.{Assert, AfterClass, BeforeClass, Test}
import com.attivio.model.query.{QueryString, PhraseQuery, Query}
import AttivioScalaSparkUtil.setDocumentField
import org.apache.spark.SparkContext._
import com.attivio.TestUtils

/**
 * Created by vijay on 12/23/2014.
 */
class ResponseDocumentRDDTest extends Serializable {

  def getSparkContext() : SparkContext =  {
    val sparkConfig = new SparkConf()
    sparkConfig.set(AttivioScalaSparkUtil.ATTIVIO_SEARCHERS, ResponseDocumentRDDTest.ATTIVIO_HOSTNAME + ":" + ResponseDocumentRDDTest.ATTIVIO_BASEPORT)
    sparkConfig.set(AttivioScalaSparkUtil.ATTIVIO_PROCESSORS, ResponseDocumentRDDTest.ATTIVIO_HOSTNAME + ":" + ResponseDocumentRDDTest.ATTIVIO_BASEPORT)
    return new SparkContext(ResponseDocumentRDDTest.MASTER, "test", sparkConfig)
  }


  @Test
  def sqlTest(): Unit = {
    val sc = getSparkContext()
    val ac = AttivioScalaSparkUtil.getAttivioConfig(sc.getConf)
    try {
      val rdd = new ResponseDocumentRDD(sc, new QueryRequest("table:city"), Array[Query](new QueryString("longitude:[* TO 0}"), new QueryString("longitude:[0 TO *]")))
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val citySqlRdd = AttivioScalaSparkUtil.docsToTable(rdd, sqlContext, ac, Array[(String, Boolean)]((".id", false),("title", false),("position", false)), "city")
      val results = sqlContext.sql("select * from city where title = 'Boston'")
      val resultsLocal = results.collect()
      Assert.assertEquals("expect 1 row", resultsLocal.size, 1)
      Assert.assertTrue("id should be string", resultsLocal(0)(0).isInstanceOf[String])
      Assert.assertTrue("title should be string", resultsLocal(0)(1).isInstanceOf[String])
      Assert.assertTrue("point.x should be double", resultsLocal(0)(2).isInstanceOf[Double])
      Assert.assertTrue("point.y should be double", resultsLocal(0)(3).isInstanceOf[Double])
      val resultsById = sqlContext.sql("select * from city where aie_doc_id = 'CITY-United States-Boston'")
      Assert.assertEquals("expect 1 row", resultsById.count(), 1)
    } finally {
      sc.stop()
    }

  }
  @Test
  def wordCountTest(): Unit = {

    val sc = getSparkContext()
    val ac = AttivioScalaSparkUtil.getAttivioConfig(sc.getConf)
    try {
      val ingestClient = AttivioScalaSparkUtil.createIngestClient(ac)
      // cleanup
      ingestClient.deleteByQuery("search", new QueryString("table:wordcount"))
      ingestClient.commit
      ingestClient.waitForCompletion
      /**
       * transform tuple into attivio document
       */
      def wordcountToDoc(wordcount : (String, Int)):AttivioDocument = {
        val doc = new AttivioDocument("wordcount_"+wordcount._1);
        setDocumentField(doc, "text", wordcount._1)
        setDocumentField(doc, "count_i", wordcount._2)
        setDocumentField(doc, "table", "wordcount")
        return doc
      }
      /**
       * feed an entire partition
       * @param iter
       */
      def ingestDocs(iter : Iterator[AttivioDocument]):Unit = {
        val ingestClient = AttivioScalaSparkUtil.createIngestClient(ac)
        ingestClient.feed(iter.toArray:_*)
        ingestClient.disconnect
      }
      // retrieve cities
      val rdd = AttivioScalaSparkUtil.searchRdd(sc, "table:city", Array[String]("title", "text"))
      // wordcount on text, feed
      val counts = rdd.flatMap(rd => rd.getFirstValueAsString("text").split(" ")).map(word => (word, 1)).reduceByKey(_ + _).map(wordcountToDoc).foreachPartition(ingestDocs)
      // commit
      ingestClient.commit
      ingestClient.waitForCompletion
      // verify records added
      val searchClient = AttivioScalaSparkUtil.createSearchClient(ac)
      val qr = searchClient.search("table:wordcount")
      System.out.println("wordCountTest - wordcount records: " + qr.getDocuments.getNumberFound)
      assertTrue("expect wordcount records", qr.getDocuments.getNumberFound > 0)
    } finally {
      sc.stop()
    }
  }


  @Test
  def testDriverRdd(): Unit = {
    val sc = getSparkContext()
    try {
      val rdd = AttivioScalaSparkUtil.searchRdd(sc, "table:city", Array[String]("title", "text"))
      System.out.println("testDriverRdd " + rdd.count())
      assertTrue(rdd.count() > 100)
    } finally {
      sc.stop()
    }
  }
  @Test
  def testPartitionById() {
    val sc = getSparkContext()
    try {
      val rdd = new ResponseDocumentRDD(sc, new QueryRequest("table:city"), 2)
      System.out.println("testPartitionById " + rdd.count())
      assertTrue(rdd.count() > 100)
    } finally {
      sc.stop()
    }
  }

  @Test
  def testPartitionByBucket() {
    val sc = getSparkContext()
    try {
      val rdd = new ResponseDocumentRDD(sc, new QueryRequest("table:city"),
        Array[Query](new QueryString("longitude:[* TO 0}"), new QueryString("longitude:[0 TO *]")))
      System.out.println("testPartitionByBucket "  + rdd.count())
      assertTrue(rdd.count() > 100)
    } finally {
      sc.stop()
    }
  }
}

object ResponseDocumentRDDTest {
  val MASTER = System.getProperty("spark.master", "local")

  val ATTIVIO_BASEPORT = Integer.parseInt(System.getProperty("attivio.baseport", "16000"))

  val ATTIVIO_HOSTNAME = System.getProperty("attivio.host", "localhost")

  val ATTIVIO_HOME = System.getProperty("attivio.home", System.getenv("ATTIVIO_HOME"))

  @BeforeClass
  def setup() {
    if ("localhost".equals(ATTIVIO_HOSTNAME)) {
      PsbProperties.setProperty("log.printStackTraces", true)
      PsbProperties.setProperty("log.level", "INFO")
      PsbProperties.setProperty("attivio.project", System.getProperty("user.dir"))
      PsbProperties.setProperty("log.directory", System.getProperty("user.dir") + "/target/logs")
      PsbProperties.setProperty("data.directory", System.getProperty("user.dir") + "/target/data")
      PsbProperties.setProperty("baseport", Integer.toString(ATTIVIO_BASEPORT))
      EsbTestUtils.initializeEnvironment
      Attivio.getInstance().start(
        TestUtils.getDefaultConfiguration("./src/test/resources/factbook.xml",
          ATTIVIO_HOME + "/conf/factbook/factbook.properties",
          ATTIVIO_HOME + "/conf/factbook/schema.xml"))
      val cc = (new DefaultAieClientFactory()).createConnectorClient(ATTIVIO_HOSTNAME, ATTIVIO_BASEPORT)
      cc.startAndWait("cityConnector", -1l)
      cc.startAndWait("countryConnector", -1l)
    }
  }
  @AfterClass
  def tearDown() {
    if ("localhost".equals(ATTIVIO_HOSTNAME)) {
      Attivio.getInstance().shutdown();
    }
  }
}
