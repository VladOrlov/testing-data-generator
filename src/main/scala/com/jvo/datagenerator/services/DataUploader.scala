package com.jvo.datagenerator.services

import com.azure.messaging.eventhubs.models.CreateBatchOptions
import com.azure.messaging.eventhubs.{EventData, EventDataBatch, EventHubClientBuilder, EventHubProducerClient}
import com.jvo.datagenerator.config.Constants.{DefaultSinkSchemaVersion, SinkSchemaVersion2}
import com.jvo.datagenerator.dto.entitydata.EntityGenerationProperties
import com.jvo.datagenerator.dto.sink.{BronzeLayerEventV1, BronzeLayerEventV2}
import com.jvo.datagenerator.dto.{RecordsImportProperties, sink}
import com.jvo.datagenerator.utils.JsonObjectMapper.toJson
import org.apache.avro.generic.GenericData.Record

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.util
import scala.jdk.CollectionConverters._

object DataUploader {

  def pushToAzureEventHub(recordsImportProperties: RecordsImportProperties): Unit = {

    recordsImportProperties.getEntityMetadata.name

    implicit val entityGenerationProperties: EntityGenerationProperties = recordsImportProperties.getEntityGenerationProperties
    val producerClient: EventHubProducerClient = getEventHubProducer(entityGenerationProperties)

    logStatistic(recordsImportProperties)
    recordsImportProperties.recordsToSend
      .map((records: Seq[Record]) => convertToTextFormatEventData(records))
      .foreach((events: Seq[EventData]) => sendInBatch(producerClient, events))

    println(s"Records for Entity: ${recordsImportProperties.getEntityMetadata.name} pushed successfully!")

//    Thread.sleep(60000)
  }

  private def logStatistic(recordsImportProperties: RecordsImportProperties, delayed: Boolean = false): Unit = {

    if (delayed)
      recordsImportProperties.recordsToDelay.foreach { records =>
        println(s"Prepare to import delayed records for Entity: " +
          s"${recordsImportProperties.getEntityMetadata.name}, number of delayed records: ${records.size}")
      }
    else
      recordsImportProperties.recordsToSend.foreach { records =>
        println(s"Prepare to import records for Entity: ${recordsImportProperties.getEntityMetadata.name}, number of records to send: ${records.size}")
      }
  }

  private def convertToTextFormatEventData(records: Seq[Record])(implicit entityGenerationProperties: EntityGenerationProperties): Seq[EventData] = {
    records
      .map(_.toString)
      .map((json: String) => getBronzeLayerEvent(json))
      .map((event: Product) => new EventData(toJson(event)))
  }

  private def getBronzeLayerEvent(json: String)(implicit entityGenerationProperties: EntityGenerationProperties): Product = {
    entityGenerationProperties.sinkSchema match {
      case DefaultSinkSchemaVersion =>
        getBronzeLayerEventV1(json)
      case SinkSchemaVersion2 =>
        getBronzeLayerEventV2(json)
      case _ =>
        getBronzeLayerEventV1(json)
    }
  }

  private def getBronzeLayerEventV1(body: String)(implicit entityGenerationProperties: EntityGenerationProperties) = {

    val dateTime = LocalDateTime.now()

    BronzeLayerEventV1(
      EntityName = entityGenerationProperties.entityName,
      EventType = entityGenerationProperties.eventType,
      MetadataVersion = 1,
      ChangeDate = Timestamp.from(dateTime.minusMinutes(1).toInstant(ZoneOffset.UTC)),
      ReceivedDate = Timestamp.from(dateTime.minusSeconds(10).toInstant(ZoneOffset.UTC)),
      ProcessingDate = Timestamp.from(dateTime.toInstant(ZoneOffset.UTC)),
      BatchId = 1,
      Body = body
    )
  }

  private def getBronzeLayerEventV2(body: String)(implicit entityGenerationProperties: EntityGenerationProperties) = {

    val dateTime = LocalDateTime.now()

    BronzeLayerEventV2(
      EntityId = entityGenerationProperties.eventType,
      EntityName = entityGenerationProperties.entityName,
      EntityVersion = 1,
      EventType = entityGenerationProperties.eventType,
      ProcessingDateTime = Timestamp.from(dateTime.toInstant(ZoneOffset.UTC)),
      BatchId = 1,
      Data = body
    )
  }

  private def convertToByteFormatEventData(records: Seq[Record]): util.List[EventData] = {
    records
      .map(_.toString)
      .map(json => new EventData(json.getBytes(StandardCharsets.UTF_8)))
      .asJava
  }

  private def getEventHubProducer(entityGenerationProperties: EntityGenerationProperties): EventHubProducerClient = {
    new EventHubClientBuilder()
      .connectionString(entityGenerationProperties.getConnectionString, entityGenerationProperties.eventHubName)
      .buildProducerClient()
  }


  private def sendInBatch(producerClient: EventHubProducerClient, events: Seq[EventData]): Unit = {
    val options: CreateBatchOptions = new CreateBatchOptions() //.setPartitionId("bla")
    var currentBatch: EventDataBatch = producerClient.createBatch()

    println("Commencing events' push to azure event-hub")
    for (event <- events) {
      if (!currentBatch.tryAdd(event)) {
        println(s"Pushing batch of events to azure - number of events: ${currentBatch.getCount}; size in bytes ${currentBatch.getSizeInBytes}")
        producerClient.send(currentBatch)
        currentBatch = producerClient.createBatch(options)
        currentBatch.tryAdd(event)
      }
    }
    producerClient.send(currentBatch)
    println("Finish events push to azure event-hub")
  }

}
