package com.attivio.platform.modules.spark

;

import com.google.common.base.Strings
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import com.attivio.messages.QueryRequest
import com.attivio.messages.QueryRequest.FacetFinderMode
import com.attivio.messages.StreamingQueryRequest
import com.attivio.messages.StreamingQueryRequest.DocumentStreamingMode
import com.attivio.model.document.{AttivioDocument, ResponseDocument}
import com.attivio.sdk.client.{IngestClient, DefaultAieClientFactory, SearchClient}
import scala.collection.mutable
import scala.util.Random
import org.apache.log4j.{LogManager, Logger}



object AttivioScalaSparkUtil {
  val log = LogManager.getLogger(AttivioScalaSparkUtil.getClass)
  def searchRdd(sc: SparkContext, query: String, fieldNames: Array[String]): RDD[ResponseDocument] = {
    val qr = new QueryRequest(query)
    qr.setFields(fieldNames:_*)
    return sc.parallelize(streamDocuments(getAttivioConfig(sc.getConf), qr).toSeq)
  }

  def setDocumentField(doc: AttivioDocument, fieldName: String, fieldValue: Any):AttivioDocument = {
    doc.setField(fieldName, Array[Any](fieldValue):_*)
    return doc
  }

  /**
   * create a search client. Parameterized by SparkConf properties:
   * <ul>
   * <li>attivio.searchers - required - comma separated list of [host]:[baseport] searchers
   * <li>attivio.usessl - optional - if true, use ssl
   * <li>attivio.username - optional - use this username
   * <li>attivio.password - optional - use this password
   * </ul>
   * @param sc
   * @return
   */
  def createSearchClient(sc: java.util.Map[String, String]): SearchClient = {
    return getAieClient(sc, "attivio.searchers", (fac, host, port) => fac.createSearchClient(host, port))
  }

  /**
   * create a search client. Parameterized by SparkConf properties:
   * <ul>
   * <li>attivio.processors - required - comma separated list of [host]:[baseport] searchers
   * <li>attivio.usessl - optional - if true, use ssl
   * <li>attivio.username - optional - use this username
   * <li>attivio.password - optional - use this password
   * </ul>
   * @param sc
   * @return
   */
  def createIngestClient(sc: java.util.Map[String, String]): IngestClient = {
    return getAieClient(sc, "attivio.processors", (fac, host, port) => fac.createIngestClient(host, port))
  }

  /**
   * generic function to create an attivio sdk client using one of the addresses specified in key.
   *
   * TODO: pick a
   *
   * @param sc
   * @param key
   * @param facFunc
   * @tparam T
   * @return
   */
  def getAieClient[T](sc: java.util.Map[String, String], key: String, facFunc: (DefaultAieClientFactory, String, Int) => T): T = {
    if(log.isDebugEnabled)
      log.debug(String.format("getAieClient config:%s", sc))
    val useSSL = "true".equals(sc.get("attivio.usessl"))
    val username = sc.get("attivio.username")
    val password = sc.get("attivio.password")
    val searchers = sc.get(key)
    require(!Strings.isNullOrEmpty(searchers))
    val searcherArray = searchers.split(",")
    require(searcherArray.size > 0)
    val hostPort = searcherArray(Random.nextInt % searcherArray.length)
    require(!Strings.isNullOrEmpty(hostPort))
    val hostPortArray = hostPort.split(":")
    require(hostPortArray.size == 2)
    val fac = new DefaultAieClientFactory()
    fac.setUseSSL(useSSL)
    if (!Strings.isNullOrEmpty(username))
      fac.setConnectionCredentials(username, password)
    if(log.isInfoEnabled)
      log.info(String.format("getAieClient host:%s, port:%s", hostPortArray:_*))
    return facFunc(fac, hostPortArray(0), hostPortArray(1).toInt)
  }

  def getAttivioConfig(sc: SparkConf): java.util.Map[String, String] = {
    val map = new java.util.HashMap[String, String]
    sc.getAll.filter(p => p._1.startsWith("attivio.")).foreach(p => map.put(p._1, p._2))
    return map
  }

  /**
   * run the query as a streaming query, do something with the results
   *
   * @param sc attivio parameters
   * @param qr query request
   */
  def streamDocuments(sc: java.util.Map[String, String], qr: QueryRequest) : Array[ResponseDocument] = {
    val arr = new mutable.ArrayBuffer[ResponseDocument]
    streamDocuments(sc, qr, iter => while(iter.hasNext) arr += iter.next)
    return arr.toArray
  }
  /**
   * run the query as a streaming query, do something with the results
   *
   * @param sc attivio parameters
   * @param qr query request
   * @param func function to apply to iterator over response documents
   */
  def streamDocuments(sc: java.util.Map[String, String], qr: QueryRequest, func: Iterator[ResponseDocument] => Unit): Unit = {
    val queryRequest = qr.clone
    // create streaming query request
    queryRequest.setRows(Long.MaxValue)
    queryRequest.setRelevancyModelName("noop")
    val streamingRequest = new StreamingQueryRequest(queryRequest)
    streamingRequest.setStreamFacets(false)
    streamingRequest.setDocumentStreamingMode(DocumentStreamingMode.FULL_DOCUMENTS)
    // search
    val searchClient = AttivioScalaSparkUtil.createSearchClient(sc)
    val streamingResponse = searchClient.search(streamingRequest)
    try {
      // iterate through results, spread across partitions
      val docIdIter = streamingResponse.getDocuments.iterator()
      func(docIdIter)
    } finally {
      if (streamingResponse != null)
        streamingResponse.close()
    }
  }

}