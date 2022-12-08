package com.jvo.datagenerator.api

import com.jvo.datagenerator.config.DataGenerationScenario
import com.jvo.datagenerator.dto.RecordsImportProperties
import com.jvo.datagenerator.dto.api.DataGenerationRequest
import com.jvo.datagenerator.dto.entitydata.EntityFields
import com.jvo.datagenerator.services.DataGenerationManager
import com.jvo.datagenerator.services.keepers.DataKeeper
import com.jvo.datagenerator.utils.AvroUtils
import org.apache.avro.generic.GenericData
import wvlet.airframe.http.HttpMessage.{Response, StringMessage}
import wvlet.airframe.http.{Endpoint, HttpMethod, HttpStatus}


@Endpoint(path = "/v1")
trait DataGenerationController {

  @Endpoint(method = HttpMethod.POST, path = "/generated-data")
  def generateData(dataGenerationRequest: DataGenerationRequest): Response = {

    DataGenerationManager.generateData(dataGenerationRequest) match {
      case Right(generatedRecord: Seq[(String, IndexedSeq[GenericData.Record])]) =>
        Response(
          status = HttpStatus.Ok_200,
          message = StringMessage(s"Data generated successfully for entities: ${generatedRecord.map(_._1).mkString(", ")}")
        )
      case Left(exceptions) =>
        Response(
          status = HttpStatus.BadRequest_400,
          message = StringMessage(exceptions.map(_.getMessage).mkString("; "))
        )
    }
  }

  @Endpoint(method = HttpMethod.PUT, path = "/generated-data")
  def updateData(entitiesToUpdate: List[EntityFields]): Response = {

    DataGenerationManager.updateGeneratedData(entitiesToUpdate)

    Response(status = HttpStatus.Ok_200, message = StringMessage("Updated"))
  }

  @Endpoint(method = HttpMethod.POST, path = "/generated-data/delayed")
  def pushDelayedData: Seq[(String, Int)] = {
    DataGenerationManager.pushDelayedData().toSeq
  }

}
