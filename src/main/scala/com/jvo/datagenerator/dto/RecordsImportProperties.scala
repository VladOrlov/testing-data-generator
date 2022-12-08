package com.jvo.datagenerator.dto

import com.jvo.datagenerator.config.{DataGenerationScenario, PersistenceMode}
import com.jvo.datagenerator.dto.entitydata.{EntityGenerationProperties, EntityMetadata, EntityMetadataGenerationProperties}
import org.apache.avro.generic.GenericData.Record

case class RecordsImportProperties(recordsToSend: Option[Map[DataGenerationScenario, Seq[Record]]],
                                   recordsToDelay: Option[Map[DataGenerationScenario, Seq[Record]]] = None,
                                   scenarioRecords: Map[DataGenerationScenario, Seq[Record]],
                                   entityMetadataGenerationProperties: EntityMetadataGenerationProperties)


object RecordsImportProperties {

  implicit class ReachRecordsImportProperties(recordsImportProperties: RecordsImportProperties) {
    def hasDataDelayScenario: Boolean = {
      recordsImportProperties.entityMetadataGenerationProperties.hasDataDelayScenario
    }

    def getEntityMetadata: EntityMetadata = {
      recordsImportProperties.entityMetadataGenerationProperties.entityMetadata
    }

    def getDataPersistenceMode: PersistenceMode = {
      recordsImportProperties.entityMetadataGenerationProperties
        .entityGenerationProperties.dataPersistenceMode
    }

    def getEntityGenerationProperties: EntityGenerationProperties = {
      recordsImportProperties.entityMetadataGenerationProperties.entityGenerationProperties
    }
  }

}
