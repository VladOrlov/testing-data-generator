package com.jvo.datagenerator.dto.entitydata

import com.jvo.datagenerator.config.Constants.DefaultSinkSchemaVersion
import com.jvo.datagenerator.config.{Basic, DataDelay, InMemory, PersistenceMode}

case class EntityGenerationProperties(server: String,
                                      sinkType: String,
                                      sinkSchema: String = DefaultSinkSchemaVersion,
                                      dataFormat: String,
                                      mode: String,
                                      dataPersistenceMode: PersistenceMode = InMemory,
                                      entityGenerationScenarioProperties: Seq[EntityGenerationScenarioProperties],
                                      recordsLimit: Int,
                                      recordsRatePerSecond: Option[Int],
                                      entityName: String,
                                      eventType: String,
                                      eventHubName: String,
                                      sharedAccessKey: String,
                                      sharedAccessKeyName: String)

object EntityGenerationProperties {

  implicit class RichEntityGenerationProperties(generationProperties: EntityGenerationProperties) {

    def getConnectionString: String = {
      s"Endpoint=sb://${generationProperties.server}.servicebus.windows.net/;" +
        s"SharedAccessKeyName=${generationProperties.sharedAccessKeyName};" +
        s"SharedAccessKey=${generationProperties.sharedAccessKey}"
    }

    def hasDataDelayScenario: Boolean = {
      generationProperties.entityGenerationScenarioProperties
        .exists(_.dataGenerationScenario == DataDelay)
    }

    def getSpecificScenariosRecordsNumber: Int = {
      generationProperties.entityGenerationScenarioProperties
        .filter(_.dataGenerationScenario != Basic)
        .map(_.recordsNumber)
        .sum
    }
  }
}