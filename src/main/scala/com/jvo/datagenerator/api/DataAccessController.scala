package com.jvo.datagenerator.api

import com.jvo.datagenerator.dto.sink.PersistenceEntity
import com.jvo.datagenerator.services.PersistenceClient
import com.jvo.datagenerator.services.keepers.DataKeeper
import com.jvo.datagenerator.utils.JsonObjectMapper
import org.apache.avro.generic.GenericData
import wvlet.airframe.http.{Endpoint, HttpMethod}

@Endpoint(path = "/v1/entities")
trait DataAccessController {

  @Endpoint(method = HttpMethod.GET, path = "/:entityName")
  def getData(entityName: String): Seq[Map[String, Any]] = {

    val (errors, records: Seq[Map[String, Any]]) = DataKeeper.getGeneratedData(entityName)
      .map((f: GenericData.Record) => f.toString)
      .map(json => JsonObjectMapper.toMap(json))
      .partitionMap(identity)

    records
  }

  @Endpoint(method = HttpMethod.GET, path = "/")
  def getEntityData(): Seq[String] = {
    val value = DataKeeper.getDataEntities
    value
  }

  @Endpoint(method = HttpMethod.GET, path = "/unpublished")
  def getUnpublishedData(): Seq[PersistenceEntity] = {
    PersistenceClient.getAllUnpublishedRecords() match {
      case Right(value) => value
      case Left(_) => Nil
    }
  }

  @Endpoint(method = HttpMethod.GET, path = "/unpublished/:entityName")
  def getUnpublishedEntityData(entityName: String): Seq[PersistenceEntity] = {
    PersistenceClient.getUnpublishedEntityRecords(entityName) match {
      case Right(value) => value
      case Left(_) => Nil
    }

  }
}
