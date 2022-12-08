package com.jvo.datagenerator.services

import com.jvo.datagenerator.config.DataGenerationScenario
import com.jvo.datagenerator.dto.RecordsImportProperties
import com.jvo.datagenerator.dto.config.ApplicationParameters
import com.jvo.datagenerator.dto.entitydata.EntityMetadata
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

  def getAllUnpublishedRecords: Either[Throwable, Seq[PersistenceEntity]] = {

    val response = requests.get(s"http://$serverUrl/v1/entities/filter?published=false")

    JsonObjectMapper.parseToPersistenceEntityList(response.text())
  }

  def saveEntityRecords(recordsImportProperties: RecordsImportProperties): Int = {

    val entities: List[PersistenceEntity] =
      mapScenarioRecordsToPersistenceEntities(recordsImportProperties.recordsToSend, recordsImportProperties) ++
        mapScenarioRecordsToPersistenceEntities(recordsImportProperties.recordsToDelay, recordsImportProperties)

    val headers: Iterable[(String, String)] = Seq(("Content-Type", "application/json"))

    val response = requests.post(
      url = s"http://$serverUrl/v1/entities/",
      headers = headers,
      data = JsonObjectMapper.toJson(entities),
      readTimeout = Int.MaxValue,
      connectTimeout = Int.MaxValue)

    response.statusCode
  }

  private def mapScenarioRecordsToPersistenceEntities(scenarioRecords: Option[Map[DataGenerationScenario, Seq[GenericData.Record]]],
                                                      recordsImportProperties: RecordsImportProperties): List[PersistenceEntity] = {
    scenarioRecords.map(_.flatMap {
      case (_, records) => records
        .map(record => mapToPersistenceEntity(record, recordsImportProperties.getEntityMetadata))
    }
      .toList)
      .getOrElse(Nil)
  }

  //TODO change to get id field name from EntityMetadata and metadata version from generation properties
  private def mapToPersistenceEntity(record: GenericData.Record, entityMetadata: EntityMetadata) = {
    PersistenceEntity(
      id = record.get(entityMetadata.idFieldName).toString,
      entityName = entityMetadata.name,
      metadataVersion = "v1",
      published = true,
      body = AvroUtils.convertRecordToMap(record)
    )
  }
}
