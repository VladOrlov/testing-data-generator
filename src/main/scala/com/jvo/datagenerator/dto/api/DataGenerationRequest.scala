package com.jvo.datagenerator.dto.api
import com.jvo.datagenerator.config.Constants._

case class DataGenerationRequest(server: String,
                                 sinkType: String,
                                 sinkSchema: String = DefaultSinkSchemaVersion,
                                 dataFormat: String,
                                 mode: String,
                                 persistenceMode: String = "in_memory",
                                 dataGenerationScenarios: List[DataGenerationScenarioRequest] = Nil,
                                 eventHubName: Option[String],
                                 sharedAccessKey: Option[String],
                                 sharedAccessKeyName: Option[String],
                                 recordsLimit: Option[Int],
                                 recordsRatePerSecond: Option[Int],
                                 entitiesGenerationProperties: List[DataGenerationProperties])

object DataGenerationRequest {

  implicit class RichDataGenerationRequest(dataGenerationRequest: DataGenerationRequest) {

    def getDataGenerationScenarioMap: Map[String, Seq[DataGenerationScenarioRequest]] = {
      dataGenerationRequest.entitiesGenerationProperties
        .flatMap(generationProperties => generationProperties.dataGenerationScenarios.map(scenario => (generationProperties.entityName, scenario)))
        .toMap
    }
  }
}
