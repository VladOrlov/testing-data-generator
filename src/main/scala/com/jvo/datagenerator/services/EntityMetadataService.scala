package com.jvo.datagenerator.services

import com.jvo.datagenerator.dto.SpecificDataFieldProperties
import com.jvo.datagenerator.dto.api.{DependencyProperties, EntityRegistration, EntityRegistrationRequest, EntityWithSchemaRequest}
import com.jvo.datagenerator.dto.entitydata._
import com.jvo.datagenerator.services.EntityMetadataService.mapSchemaToEntityMetadata
import com.jvo.datagenerator.services.keepers.RolesKeeper.GenericRole
import com.jvo.datagenerator.services.keepers.{EntitiesKeeper, SchemasKeeper}
import com.jvo.datagenerator.utils.AvroUtils
import org.apache.avro.Schema

import scala.util.Try

object EntityMetadataService {

  case class FieldDependencyMetadata(dependentField: DependentField, entityMetadata: EntityMetadata)

  //TODO Investigate purpose of method
  def registerEntity(entityMetadata: EntityMetadata): Unit = {

    val (errors, fieldsDependenciesMetadata: Set[FieldDependencyMetadata]) =
      entityMetadata.dependentFields
        .map(dependentField => getDependencyEntity(dependentField))
        .map(maybeFieldDependencyEntity => getDependencyField(maybeFieldDependencyEntity))
        .partitionMap(identity)

    val entityMetadataWithDependencies = EntityMetadataWithDependencies(entityMetadata, fieldsDependenciesMetadata)

    EntitiesKeeper.addEntity(entityMetadata)

  }

  private def fieldHasSameTypes(dependencyEntityField: Schema.Field, field: Schema.Field) = {
    dependencyEntityField.schema().getType == field.schema().getType
  }

  private def getDependencyField(maybeFieldDependencyEntity: Either[IllegalArgumentException, FieldDependencyMetadata]) = {
    maybeFieldDependencyEntity
      .flatMap { fieldDependencyMetadata =>
        Option(dependencyFieldEntitySchema(fieldDependencyMetadata).getField(dependencyFieldName(fieldDependencyMetadata)))
          .map { dependencyEntityField =>
            if (fieldHasSameTypes(dependencyEntityField, getDependentField(fieldDependencyMetadata)))
              Right(fieldDependencyMetadata)
            else
              Left(new IllegalArgumentException("Wrong type"))
          }
          .getOrElse(getIllegalArgumentException(fieldDependencyMetadata))
      }
  }

  private def getDependentField(fieldDependencyMetadata: FieldDependencyMetadata) = {
    fieldDependencyMetadata.dependentField.field
  }

  private def getIllegalArgumentException(dependencyFieldMetadata: FieldDependencyMetadata) = {
    Left(new IllegalArgumentException(
      s"No field named ${dependencyFieldName(dependencyFieldMetadata)} " +
        s"found for entity ${dependencyFieldEntitySchema(dependencyFieldMetadata).getName}!")
    )
  }

  private def dependencyFieldEntitySchema(dependencyFieldMetadata: FieldDependencyMetadata) = {
    dependencyFieldMetadata.entityMetadata.schema
  }

  private def dependencyFieldName(dependencyFieldMetadata: FieldDependencyMetadata) = {
    dependencyFieldMetadata.dependentField.dependencyField
  }

  private def getDependencyEntity(dependentField: DependentField) = {
    EntitiesKeeper.getEntity(dependentField.dependencyEntity)
      .map(entityMetadata => FieldDependencyMetadata(dependentField, entityMetadata))
  }

  def mapToEntityMetadata(entityRegistration: EntityRegistrationRequest): Either[Throwable, EntityMetadata] = {

    val entity = SchemasKeeper.getSchema(entityRegistration.schemaName)
      .flatMap(schema =>  mapSchemaToEntityMetadata(schema, entityRegistration))

    entity
  }

  def mapToEntityMetadata(registrationRequest: EntityWithSchemaRequest): Either[Throwable, EntityMetadata] = {

    val entity = Try(AvroUtils.parseSchema(registrationRequest.schemaJson))
      .toEither
      .flatMap(schema =>  mapSchemaToEntityMetadata(schema, registrationRequest))

    entity
  }

  private def mapSchemaToEntityMetadata(schema: Schema, registrationRequest: EntityRegistration) = {
      val (dependentFieldsErrors, dependentFields) = getDependentFields(registrationRequest.getDependentFields(), schema)
      val (specificFieldsErrors, specificFields) = getSpecificDataFields(registrationRequest.getSpecificDataFields(), schema)

      if (dependentFieldsErrors.isEmpty && specificFieldsErrors.isEmpty) {
        Right(EntityMetadata(
          name = registrationRequest.getName(),
          role = registrationRequest.getRole().getOrElse(GenericRole),
          schema = schema,
          dependentFields = dependentFields,
          specificDataFields = specificFields
        ))
      } else {
        Left(new IllegalArgumentException((dependentFieldsErrors ++ specificFieldsErrors)
          .head
          .getMessage))
      }
  }

  private def getDependentFields(dependentFields: Option[Map[String, DependencyProperties]], schema: Schema) = {
    dependentFields
      .map(_.map {
        case (fieldName, properties) => Try(schema.getField(fieldName))
          .toEither
          .map(field => DependentField(field, properties.dependencyEntity, properties.dependencyField))
      })
      .toSet
      .flatten
      .partitionMap(identity)
  }

  private def getSpecificDataFields(specificDataFields: Option[Map[String, String]], schema: Schema) = {
    specificDataFields
      .map(_.map {
        case (fieldName, fieldRole) => Try(schema.getField(fieldName))
          .toEither
          .map(field => SpecificDataFieldProperties(field, fieldRole))
      })
      .toSet
      .flatten
      .partitionMap(identity)
  }

}
