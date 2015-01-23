package com.attivio.platform.modules.spark

import com.attivio.messages.StreamingQueryRequest.DocumentStreamingMode
import com.attivio.messages.{StreamingQueryRequest, QueryRequest}
import com.attivio.model.FieldNames
import com.attivio.model.document.ResponseDocument
import com.attivio.model.query.phrase.PhraseOr
import com.attivio.model.query.{PhraseQuery, BooleanOrQuery}
import com.attivio.model.request.filter.QueryFilter
import com.attivio.sdk.client.{SearchClient, DefaultAieClientFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import com.attivio.model.query.Query

/**
 * partition with a set of document ids
 *
 * @param idx partition index
 * @param queryRequest queryRequest with filter set
 * @param docIds
 */
private class ResponseDocumentIdPartition(idx: Int, queryRequest: QueryRequest, docIds: Array[String]) extends Partition {
  override def index = idx
  def documentIds: Array[String] = docIds
}

/**
 * partition based on buckets or bins
 *
 * @param idx
 * @param qr
 */
private class ResponseDocumentFilterPartition(idx: Int, qr: QueryRequest) extends Partition {
  override def index = idx
  def queryRequest: QueryRequest = qr
}

/**
 * distributed iterator for iterating through document ids and getting ResponseDocuments
 *
 * @param queryRequest
 * @param documentIds

 */
class ResponseDocumentIdPartitionIterator(attivioConf: java.util.Map[String, String], queryRequest: QueryRequest, documentIds: Array[String]) extends Iterator[ResponseDocument] {
  val queue = new mutable.Queue[ResponseDocument]
  var currentIndex : Int = 0


  override def hasNext: Boolean = {
    !queue.isEmpty || currentIndex < documentIds.length
  }

  override def next(): ResponseDocument = {
    if(queue.isEmpty) {
      var endIndex = currentIndex+100
      if(endIndex > documentIds.length)
        endIndex = documentIds.length
      val qr = queryRequest.clone
      qr.setRows(Long.MaxValue)
      qr.addFilter(new PhraseQuery(FieldNames.ID, new PhraseOr(documentIds.slice(currentIndex, endIndex):_*)))
      AttivioScalaSparkUtil.streamDocuments(attivioConf, qr, iter => while(iter.hasNext) queue.enqueue(iter.next))
      currentIndex = endIndex
    }
    queue.dequeue()
  }
}

/**
 *
 */
class ResponseDocumentRDD(sc: SparkContext,
                              queryRequest: QueryRequest,
                              queryFilters: Array[Query],
                              numPartitions: Int,
                              attivioConfig: java.util.Map[String, String])
    extends RDD[ResponseDocument](sc, Nil) with Logging {

  /**
   * partition by id
   * @param sc
   * @param queryRequest
   * @param numPartitions

   */
  def this(sc: SparkContext,
           queryRequest: QueryRequest,
           numPartitions: Int) {
    this(sc, queryRequest, null, numPartitions, AttivioScalaSparkUtil.getAttivioConfig(sc.getConf))
  }

  /**
   * partition by bucket - buckets defined by queryFilters
   * @param sc
   * @param queryRequest
   * @param queryFilters

   */
  def this(sc: SparkContext,
           queryRequest: QueryRequest,
           queryFilters: Array[Query]) {
    this(sc, queryRequest, queryFilters, queryFilters.length, AttivioScalaSparkUtil.getAttivioConfig(sc.getConf))
  }

  override def getPartitions: Array[Partition] = {
    if(queryFilters != null)
      return getPartitionsByBucket
    else
      return getPartitionsById
  }

  def getPartitionsByBucket: Array[Partition] = {
    def createResponseDocumentFilterPartition(i:Int) : Partition = {
      val qr = queryRequest.clone()
      qr.addFilter(queryFilters(i))
      new ResponseDocumentFilterPartition(i, qr)
    }
    return (0 until numPartitions).map(i => createResponseDocumentFilterPartition(i)).toArray
  }
  /**
   * run streaming query load document ids into each partition
   * @return
   */
  def getPartitionsById: Array[Partition] = {
    // allocate arrayBuffers for the doc ids per partition
    val partitionDocIds = (0 until numPartitions).map(i => new ArrayBuffer[String]).toArray
    // create streaming query request
    queryRequest.setRows(Long.MaxValue)
    val streamingRequest = new StreamingQueryRequest(queryRequest)
    streamingRequest.setStreamFacets(false)
    streamingRequest.setDocumentStreamingMode(DocumentStreamingMode.DOCUMENT_IDS_ONLY)
    // search
    val searchClient = AttivioScalaSparkUtil.createSearchClient(attivioConfig)
    val streamingResponse = searchClient.search(streamingRequest)
    try {
      // iterate through results, spread across partitions
      var counter = 0 : Int
      val docIdIter = streamingResponse.getDocumentIds.iterator()
      while(docIdIter.hasNext) {
        partitionDocIds(counter % numPartitions).+=(docIdIter.next())
        counter = counter+1
      }
      log.info("id count: {}", counter)
    } finally {
      if(streamingResponse != null)
        streamingResponse.close()
    }
    // convert arraybuffers into partitions
    (0 until numPartitions).map( i => new ResponseDocumentIdPartition(i, queryRequest, partitionDocIds(i).toArray)).toArray
  }

  override def compute(thePart: Partition, context: TaskContext) : Iterator[ResponseDocument] = {
    assert ( thePart.isInstanceOf[ResponseDocumentIdPartition] || thePart.isInstanceOf[ResponseDocumentFilterPartition])
    if(thePart.isInstanceOf[ResponseDocumentIdPartition]) {
      return new ResponseDocumentIdPartitionIterator(attivioConfig, queryRequest, thePart.asInstanceOf[ResponseDocumentIdPartition].documentIds)
    } else  {
      return AttivioScalaSparkUtil.streamDocuments(attivioConfig, thePart.asInstanceOf[ResponseDocumentFilterPartition].queryRequest).iterator
    }
  }

}
