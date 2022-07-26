package com.jvo.datagenerator.dto

import com.jvo.datagenerator.config.PersistenceMode
import com.jvo.datagenerator.dto.entitydata.{EntityGenerationProperties, EntityMetadata, EntityMetadataGenerationProperties}
import org.apache.avro.generic.GenericData

case class RecordsImportProperties(recordsToSend: Option[Seq[GenericData.Record]],
                                   recordsToDelay: Option[Seq[GenericData.Record]] = None,
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

  def apply(recordsToSend: Seq[GenericData.Record], entityMetadataGenerationProperties: EntityMetadataGenerationProperties): RecordsImportProperties = {
    RecordsImportProperties(
      recordsToSend = Option(recordsToSend),
      entityMetadataGenerationProperties = entityMetadataGenerationProperties
    )
  }
}
