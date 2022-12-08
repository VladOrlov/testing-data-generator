package com.jvo.datagenerator.dto.entitydata

import com.jvo.datagenerator.config.DataGenerationScenario

case class EntityMetadataGenerationProperties(entityMetadata: EntityMetadata,
                                              entityGenerationProperties: EntityGenerationProperties)


object EntityMetadataGenerationProperties {

  implicit class EnrichedEntityMetadataGenerationProperties(entityMetadataGenerationProperties: EntityMetadataGenerationProperties) {

    def getEntitiesByScenario(scenario: DataGenerationScenario): Seq[String] = {
      getEntityGenerationScenarioProperties
        .filter(_.dataGenerationScenario == scenario)
        .flatMap((props: EntityGenerationScenarioProperties) => props.entityFields.map(_.entityName))
    }

    def hasDataDelayScenario: Boolean = {
      entityMetadataGenerationProperties.entityGenerationProperties.hasDataDelayScenario
    }

    def getRecordsSizeLimit: Int = {
      entityMetadataGenerationProperties.entityGenerationProperties.recordsLimit
    }

    def getEntityGenerationScenarioProperties: Seq[EntityGenerationScenarioProperties] = {
      entityMetadataGenerationProperties
        .entityGenerationProperties
        .entityGenerationScenarioProperties
    }
  }

}