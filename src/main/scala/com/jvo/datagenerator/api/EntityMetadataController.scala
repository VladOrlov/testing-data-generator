package com.jvo.datagenerator.api


import com.jvo.datagenerator.api.EntityMetadataController.{EntityMetadataResponse, EntityRegistrationResponse}
import com.jvo.datagenerator.dto.api.{DependencyProperties, EntityRegistrationRequest, EntityWithSchemaRequest}
import com.jvo.datagenerator.dto.entitydata.EntityMetadata
import com.jvo.datagenerator.services.EntityMetadataService
import com.jvo.datagenerator.services.keepers.EntitiesKeeper
import com.jvo.datagenerator.utils.{AvroUtils, JsonObjectMapper}
import wvlet.airframe.http.{Endpoint, HttpMethod}

import scala.util.Try

object EntityMetadataController {
  case class EntityRegistrationResponse(name: String,
                                        role: Option[String],
                                        schema: Map[String, Any],
                                        dependentFields: Option[Map[String, DependencyProperties]],
                                        specificDataFields: Option[Map[String, String]])

  case class EntityMetadataResponse(status: Int,
                                    entityMetadata: Option[EntityRegistrationResponse] = None,
                                    error: Option[String] = None)
}

@Endpoint(path = "/v1/entities-metadata")
trait EntityMetadataController {

  @Endpoint(method = HttpMethod.GET, path = "")
  def getAllEntitiesMetadata: List[EntityRegistrationResponse] = {
    val (errors, entities) = EntitiesKeeper.getAllEntities
      .map { case (id, entity) => JsonObjectMapper.toMap(entity.schema.toString())
        .map((schemaMap: Map[String, Any]) => mapToEntityRegistrationResponse(entity, schemaMap)
        )
      }.partitionMap(identity)

    entities.toList
  }

  @Endpoint(method = HttpMethod.GET, path = "/:entityName")
  def getEntityMetadata(entityName: String): EntityMetadataResponse = {
    EntitiesKeeper.getEntity(entityName)
      .flatMap { entity: EntityMetadata =>
        JsonObjectMapper.toMap(entity.schema.toString())
          .map(schemaMap => mapToEntityRegistrationResponse(entity, schemaMap))
      } match {
      case Right(entity: EntityRegistrationResponse) =>
        EntityMetadataResponse(status = 200, entityMetadata = Option(entity))
      case Left(error) =>
        EntityMetadataResponse(status = 400, error = Option(error.getMessage))
    }
  }

  @Endpoint(method = HttpMethod.POST, path = "")
  def registerNewEntity(registrationRequest: EntityRegistrationRequest): EntityMetadataResponse = {

    val throwableOrMetadata = EntityMetadataService.mapToEntityMetadata(registrationRequest)
    val maybeMetadataRegistration: Either[Throwable, EntityRegistrationResponse] = processEntityMetadataRegistration(throwableOrMetadata)

    mapToMetadataResponse(maybeMetadataRegistration)
  }

  @Endpoint(method = HttpMethod.POST, path = "/with-schema")
  def registerNewEntityWithSchema(registrationRequest: EntityWithSchemaRequest): EntityMetadataResponse = {

    val maybeMetadataRegistration: Either[Throwable, EntityRegistrationResponse] =
      Try(AvroUtils.parseSchema(registrationRequest.schemaJson)).toEither
        .flatMap { schema =>
          JsonObjectMapper.toMap(schema.toString())
            .flatMap { schemaMap =>
              val throwableOrMetadata = EntityMetadataService.mapToEntityMetadata(registrationRequest)
              processEntityMetadataRegistration(throwableOrMetadata)
            }
        }

    mapToMetadataResponse(maybeMetadataRegistration)
  }

  private def mapToMetadataResponse(maybeMetadataRegistration: Either[Throwable, EntityRegistrationResponse]) = {
    maybeMetadataRegistration match {
      case Right(registrationResponse) =>
        EntityMetadataResponse(status = 200, entityMetadata = Option(registrationResponse))
      case Left(error) =>
        EntityMetadataResponse(status = 400, error = Option(error.getMessage))
    }
  }

  private def mapToEntityRegistrationResponse(entity: EntityMetadata, schemaMap: Map[String, Any]) = {
    val specificFields = Option(entity.specificDataFields
      .map(field => (field.field.name(), field.fieldRole))
      .toMap)

    val dependentFields = Option(entity.dependentFields
      .map(dependentField => (dependentField.field.name(), DependencyProperties(dependentField.dependencyEntity, dependentField.dependencyField)))
      .toMap)

    EntityRegistrationResponse(
      name = entity.name,
      role = Option(entity.role),
      schema = schemaMap,
      dependentFields = dependentFields,
      specificDataFields = specificFields)
  }

  private def processEntityMetadataRegistration(maybeEntityMetadata: Either[Throwable, EntityMetadata]): Either[Throwable, EntityRegistrationResponse] = {
    maybeEntityMetadata.flatMap { entityMetadata =>
      EntitiesKeeper.addEntity(entityMetadata)
      JsonObjectMapper.toMap(entityMetadata.schema.toString())
        .flatMap(schemaMap => Right(mapToEntityRegistrationResponse(entityMetadata, schemaMap)))
    }
  }
}
