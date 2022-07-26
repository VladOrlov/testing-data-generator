package com.jvo.datagenerator.services.keepers

import com.jvo.datagenerator.dto.RecordsImportProperties
import com.jvo.datagenerator.dto.entitydata.{EntityDataWithGenerationContext, EntityMetadata, EntityMetadataGenerationProperties}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record

import scala.collection.concurrent.TrieMap

object DataKeeper {

  private val generatedData: TrieMap[String, EntityDataWithGenerationContext] = TrieMap()
  private val delayedData: TrieMap[String, EntityDataWithGenerationContext] = TrieMap()
  private val scenarioSpecificData: TrieMap[String, TrieMap[String, Record]] = TrieMap()

  def addRecordsToGeneratedData(records: Seq[Record])(implicit entityMetadataGenerationProperties: EntityMetadataGenerationProperties): Unit = {
    addRecordsToDataHolder(records, generatedData)
  }

  def addRecordsToGeneratedData(recordsImportProperties: RecordsImportProperties): Unit = {
    recordsImportProperties.recordsToSend
      .foreach(records => addRecordsToGeneratedData(records)(recordsImportProperties.entityMetadataGenerationProperties))
  }

  def addRecordsToDelayedData(recordsImportProperties: RecordsImportProperties): Unit = {
    recordsImportProperties.recordsToDelay
      .foreach(records => addRecordsToDelayedData(records)(recordsImportProperties.entityMetadataGenerationProperties))
  }

  def addRecordsToDelayedData(records: Seq[Record])(implicit entityMetadataGenerationProperties: EntityMetadataGenerationProperties): Unit = {
    addRecordsToDataHolder(records, delayedData)
  }

  def addRecords(recordsImportProperties: RecordsImportProperties): Unit = {
    addRecordsToGeneratedData(recordsImportProperties)
    addRecordsToDelayedData(recordsImportProperties)
  }

  private def addRecordsToDataHolder(records: Seq[Record], dataHolder: TrieMap[String, EntityDataWithGenerationContext])
                                    (implicit entityMetadataGenerationProperties: EntityMetadataGenerationProperties): Unit = {

    val entityKey = getNormalizedEntityKey(entityMetadataGenerationProperties.entityMetadata)
    val idFieldName = entityMetadataGenerationProperties.entityMetadata.idFieldName

    val entityData = dataHolder.getOrElse(entityKey,
      EntityDataWithGenerationContext(entityMetadataGenerationProperties = entityMetadataGenerationProperties)
    )

    records.foreach {
      record => entityData.generationData.addOne(record.get(idFieldName).toString, record)
    }

    dataHolder.update(entityKey, entityData)

  }

  def getAllDelayedDataMap: Map[String, RecordsImportProperties] = {
    delayedData.map { case (entityName, entityDataWithGenerationContext) =>
      (entityName, getRecordImportProperties(entityDataWithGenerationContext))
    }.toMap
  }

  def getAllDelayedEntityData(implicit entityMetadata: EntityMetadata): Option[RecordsImportProperties] = {
    val entityKey = getNormalizedEntityKey
    delayedData.get(entityKey)
      .map(entityDataWithGenerationContext =>
        getRecordImportProperties(entityDataWithGenerationContext)
      )
  }

  def removeDelayedEntityData(entityMetadata: EntityMetadata): Option[EntityDataWithGenerationContext] = {
    val entityKey = getNormalizedEntityKey(entityMetadata)
    delayedData.remove(entityKey)
  }

  private def getRecordImportProperties(entityDataWithGenerationContext: EntityDataWithGenerationContext) = {
    RecordsImportProperties(
      recordsToSend = None,
      recordsToDelay = Option(entityDataWithGenerationContext.generationData.values.toSeq),
      entityMetadataGenerationProperties = entityDataWithGenerationContext.entityMetadataGenerationProperties)
  }

  def getGeneratedData(key: String): Seq[Record] = {
    generatedData.get(getNormalizedEntityKey(key))
      .map(_.generationData.values.toSeq)
      .getOrElse(Nil)
  }

  def getGeneratedDataWithContext(key: String): Option[EntityDataWithGenerationContext] = {
    generatedData.get(getNormalizedEntityKey(key))
  }

  def getSizeByKey(key: String): Int = {
    generatedData.get(getNormalizedEntityKey(key))
      .map(_.generationData.size)
      .getOrElse(0)
  }

  def getValueForKeyByIndex(key: String, index: Int): Option[Record] = {
    generatedData.get(getNormalizedEntityKey(key))
      .map(_.generationData.values.toArray)
      .map(array => array(index))
  }

  def getDataEntities: Seq[String] = {
    generatedData.keySet.toSeq
  }

  private def getNormalizedEntityKey(implicit entityMetadata: EntityMetadata): String = {
    entityMetadata.name.toUpperCase
  }

  private def getNormalizedEntityKey(key: String): String = {
    key.toUpperCase
  }
}
