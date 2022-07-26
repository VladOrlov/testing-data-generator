package com.jvo.datagenerator.utils

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.jvo.datagenerator.dto.api.EntityRegistrationRequest
import com.jvo.datagenerator.dto.sink.PersistenceEntity
import org.apache.avro.Schema
import org.apache.logging.log4j.LogManager

import scala.util.{Failure, Success, Try}

object JsonObjectMapper {

  private[this] final val log = LogManager.getLogger(this.getClass)

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

  def parseToSchemaObject(sourceJson: String): Either[Throwable, Schema] = {
    Try(mapper.readValue[Schema](sourceJson)).toEither
  }

  def parseToEntityRegistration(sourceJson: String): Either[Throwable, EntityRegistrationRequest] = {
    Try(mapper.readValue[EntityRegistrationRequest](sourceJson)).toEither
  }

  def parseToSchemasList(sourceJson: String): Either[Throwable, Seq[Schema]] = {
    Try(mapper.readValue[Seq[Schema]](sourceJson)).toEither
  }

  def parseToPersistenceEntity(sourceJson: String): Either[Throwable, PersistenceEntity] = {
    Try(mapper.readValue[PersistenceEntity](sourceJson)).toEither
  }

  def parseToPersistenceEntityList(sourceJson: String): Either[Throwable, Seq[PersistenceEntity]] = {
    Try(mapper.readValue[Seq[PersistenceEntity]](sourceJson)).toEither
  }

  def toJson(sourceObject: Any): String = {
    Try(mapper.writeValueAsString(sourceObject)) match {
      case Success(value) => value
      case Failure(exception) =>
        log.error(exception)
        "{}"
    }
  }

  def toMap(sourceJson: String): Either[Throwable, Map[String, Any]] = {
    Try(mapper.readValue[Map[String, Any]](sourceJson)).toEither
  }

}