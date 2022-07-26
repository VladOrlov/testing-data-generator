package com.jvo.datagenerator.api

import com.jvo.datagenerator.api.SchemaController.SchemaResponseDto
import com.jvo.datagenerator.services.keepers.SchemasKeeper
import com.jvo.datagenerator.utils.{AvroUtils, JsonObjectMapper}
import wvlet.airframe.http.{Endpoint, HttpMethod}

import scala.collection.immutable
import scala.util.Try

object SchemaController {
  case class SchemaResponseDto(id: String, schema: Map[String, Any])
}

@Endpoint(path = "/v1")
trait SchemaController {

  @Endpoint(method = HttpMethod.GET, path = "/schemas")
  def getRegisteredSchemas: List[SchemaResponseDto] = {

    val (parseErrors, schemaList: immutable.Iterable[SchemaResponseDto]) = SchemasKeeper.getAllSchemas
      .map { case (schemaFullName, schema) =>
        JsonObjectMapper.toMap(schema.toString())
          .map(schemaMap => SchemaResponseDto(schemaFullName, schemaMap))
      }.partitionMap(identity)

    schemaList.toList

//    if (parseErrors.nonEmpty) {
//      Response(
//        status = HttpStatus.InternalServerError_500,
//        message = StringMessage(parseErrors.toString()))
//    } else {
//      Response(
//        status = HttpStatus.Ok_200,
//        message = StringMessage(JsonObjectMapper.toJson(schemaList)))
//    }
  }

  @Endpoint(method = HttpMethod.POST, path = "/schemas")
  def registerNewSchema(schemaJson: String): SchemaResponseDto = {
    Try(AvroUtils.parseSchema(schemaJson)).toEither match {
      case Right(schema) =>
        SchemasKeeper.addSchema(schema)

        JsonObjectMapper.toMap(schema.toString())
          .map(schemaMap => SchemaResponseDto(schema.getFullName, schemaMap))
          .getOrElse(SchemaResponseDto("400", Map("Error" -> "Bad request")))

      case Left(error) =>
        SchemaResponseDto("400", Map("Error" -> "Bad request"))
    }
  }

}
