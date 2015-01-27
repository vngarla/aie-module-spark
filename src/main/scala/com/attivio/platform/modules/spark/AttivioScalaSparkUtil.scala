package com.attivio.platform.modules.spark

;

import com.attivio.model.query.{QueryString, Query}
import com.attivio.model.schema.{Schema, SchemaField}
import com.google.common.base.Strings
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import com.attivio.messages.QueryRequest
import com.attivio.messages.StreamingQueryRequest
import com.attivio.messages.StreamingQueryRequest.DocumentStreamingMode
import com.attivio.model.document.{AttivioDocument, ResponseDocument}
import com.attivio.sdk.client.{IngestClient, DefaultAieClientFactory, SearchClient}
import scala.collection.mutable
import scala.util.Random
import org.apache.log4j.{LogManager}
import org.apache.spark.sql.SQLContext


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

  /**
   * TODO handle dynamic fields
   * @param schema
   * @param fieldName
   * @param multivalued
   * @return
   */
  def fieldToSparkType(schema: Schema, fieldName: String, multivalued: Boolean):Array[StructField] = {
    if(fieldName.equals(".id") || fieldName.equalsIgnoreCase("aie_doc_id"))
      return Array[StructField](StructField("aie_doc_id", StringType, false))
    val sf = schema.getField(fieldName)
    require(sf != null)
    val mv = (multivalued != null && multivalued) || (multivalued == null && sf.isMultiValue)
    val t = sf.getType
    // special handling for points
    if(SchemaField.Type.POINT.equals(t)) {
      if(mv) {
        return Array[StructField](StructField(fieldName + "_x", ArrayType(DoubleType), true), StructField(fieldName + "_y", ArrayType(DoubleType), true))
      } else {
        return Array[StructField](StructField(fieldName + "_x", DoubleType, true), StructField(fieldName + "_y", DoubleType, true))
      }
    }
    // handling for other types
    val sparkType = t match {
      case SchemaField.Type.BOOLEAN => BooleanType
      case SchemaField.Type.DATE => DateType
      case SchemaField.Type.DOUBLE => DoubleType
      case SchemaField.Type.FLOAT => FloatType
      case SchemaField.Type.INTEGER => IntegerType
      case SchemaField.Type.LONG => LongType
      // TODO why won't DecimalType work?
      case SchemaField.Type.DECIMAL => DoubleType
      case SchemaField.Type.MONEY => DoubleType
      case _ =>  StringType
    }
    if(mv) {
      return Array[StructField](StructField(fieldName, ArrayType(sparkType), true))
    } else {
      return Array[StructField](StructField(fieldName, sparkType, true))
    }
  }

  /**
   * create converters for the specified field.
   * @param schema required - schema name
   * @param fieldName required - field name
   * @param multivalued optional - if true will extract an array type; else will extract a single value (the first)
   * @return for all but points this will be a single-entry array.
   */
  def fieldToConverter(schema: Schema, fieldName: String, multivalued: Boolean):Array[FieldConverter] = {
    if(fieldName.equals(".id") || fieldName.equalsIgnoreCase("aie_doc_id"))
      return Array[FieldConverter](new IdFieldConverter())
    val sf = schema.getField(fieldName)
    require(sf != null)
    val mv = (multivalued != null && multivalued) || (multivalued == null && sf.isMultiValue)
    val t = sf.getType
    if(SchemaField.Type.POINT.equals(t)) {
      return Array[FieldConverter](new PointConverter(fieldName, mv, true), new PointConverter(fieldName, mv, false))
    }
    return Array[FieldConverter](new FieldConverter(fieldName, mv))
  }

  /**
   * For the given list of fields, create a Spark SQL StructType for the table definition.
   * Create an array of FieldConverters for converting attivio documents into records
   * @param sc attivio parameters
   * @param fields array of (field name, multivalued) tuples.
   * @return tuple of StructType, array of FieldConverters
   */
  def fieldsToSchema(sc: java.util.Map[String, String], fields: Array[(String, Boolean)]): (StructType, DocToRowConverter) = {
    val searchClient = AttivioScalaSparkUtil.createSearchClient(sc)
    val schema = searchClient.getDefaultSchema
    return (StructType(fields.flatMap(field => fieldToSparkType(schema, field._1, field._2))), new DocToRowConverter(fields.flatMap(field => fieldToConverter(schema, field._1, field._2))))
  }

  /**
   * Main entry point to convert an RDD of ResponseDocuments into a spark sql table.
   *
   * The specified fields will be mapped to row values, in the specified order.  The type mapping is as you would expect, modulo Money/Decimal (couldn't get that to work, TODO).
   *
   * Points will be mapped as 2 doubles - [field name]_x and [field name]_y.
   *
   * The .id field will be mapped to aie_doc_id to simplify referring to this field in SQL.  Alternatively, specify the "aie_doc_id" as the field name (we will figure out that this means the .id field)
   *
   * @param docRDD response documents from an AIE query
   * @param sqlContext sqlContext to add table to
   * @param ac attivio parameters
   * @param fields array of (fieldName, multivalued) tuples.  Often we do not explicitly specify a field as single valued in the schema.
   * @param table the table to register the rdd under
   * @return SchemaRDD of the converted results
   */
  def docsToTable(docRDD: RDD[ResponseDocument], sqlContext: SQLContext, ac: java.util.Map[String, String], fields: Array[(String, Boolean)], table: String): SchemaRDD = {
    val schemaAndConverters = AttivioScalaSparkUtil.fieldsToSchema(ac, Array[(String, Boolean)]((".id", false),("title", false),("position", false),("location", true)))
    val sqlRdd = docRDD.map(doc => schemaAndConverters._2.docToRow(doc))
    val tableSqlRdd = sqlContext.applySchema(sqlRdd, schemaAndConverters._1)
    tableSqlRdd.registerTempTable(table)
    return tableSqlRdd
  }

}