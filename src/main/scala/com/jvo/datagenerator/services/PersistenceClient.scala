package com.jvo.datagenerator.services

import com.jvo.datagenerator.dto.RecordsImportProperties
import com.jvo.datagenerator.dto.config.ApplicationParameters
import com.jvo.datagenerator.dto.sink.PersistenceEntity
import com.jvo.datagenerator.utils.{AvroUtils, JsonObjectMapper}
import org.apache.avro.generic.GenericData

object PersistenceClient {

  private var serverUrl: String = "localhost:8090"

  def init(applicationParameters: ApplicationParameters): Unit = {
    serverUrl = applicationParameters.persistenceServer
  }

  def getUnpublishedEntityRecords(entityName: String): Either[Throwable, Seq[PersistenceEntity]] = {

    val response = requests.get(s"http://$serverUrl/v1/entities/filter?entityName=$entityName&published=false")

    JsonObjectMapper.parseToPersistenceEntityList(response.text())
  }

  def getAllUnpublishedRecords(): Either[Throwable, Seq[PersistenceEntity]] = {

    val response = requests.get(s"http://$serverUrl/v1/entities/filter?published=false")

    JsonObjectMapper.parseToPersistenceEntityList(response.text())
  }

  def saveEntityRecords(recordsImportProperties: RecordsImportProperties): Int = {

    val entities: List[PersistenceEntity] = List(
      recordsImportProperties.recordsToSend
        .map(_.map(record => mapToPersistenceEntity(recordsImportProperties, record, published = true))),
      recordsImportProperties.recordsToDelay
        .map(_.map(record => mapToPersistenceEntity(recordsImportProperties, record, published = false)))
    ).flatten.flatten

    val headers: Iterable[(String, String)] = Seq(("Content-Type", "application/json"))

    val response = requests.post(
      url = s"http://$serverUrl/v1/entities/",
      headers = headers,
      data = JsonObjectMapper.toJson(entities),
      readTimeout = Int.MaxValue,
      connectTimeout = Int.MaxValue)

    response.statusCode
  }

  //TODO change to get id field name from EntityMetadata and metadata version from generation properties
  private def mapToPersistenceEntity(recordsImportProperties: RecordsImportProperties, record: GenericData.Record, published: Boolean) = {
    PersistenceEntity(
      id = record.get("id").toString,
      entityName = recordsImportProperties.getEntityMetadata.name,
      metadataVersion = "v1",
      published = published,
      body = AvroUtils.convertRecordToMap(record)
    )
  }
}
