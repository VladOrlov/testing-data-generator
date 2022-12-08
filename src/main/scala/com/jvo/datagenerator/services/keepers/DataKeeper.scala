package com.jvo.datagenerator.services.keepers

import com.jvo.datagenerator.config.DataGenerationScenario
import com.jvo.datagenerator.dto.RecordsImportProperties
import com.jvo.datagenerator.dto.entitydata.{EntityDataWithGenerationContext, EntityMetadata, EntityMetadataGenerationProperties}
import org.apache.avro.generic.GenericData.Record

import scala.collection.concurrent.TrieMap

object DataKeeper {

  private val generatedData: TrieMap[String, EntityDataWithGenerationContext] = TrieMap()
  private val delayedData: TrieMap[String, EntityDataWithGenerationContext] = TrieMap()

  def addRecordsToGeneratedData(records: Map[DataGenerationScenario, Seq[Record]])
                               (implicit entityMetadataGenerationProperties: EntityMetadataGenerationProperties): Unit = {

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

  def addRecordsToDelayedData(records: Map[DataGenerationScenario, Seq[Record]])
                             (implicit entityMetadataGenerationProperties: EntityMetadataGenerationProperties): Unit = {
    addRecordsToDataHolder(records, delayedData)
  }

  def addRecords(recordsImportProperties: RecordsImportProperties): Unit = {
    addRecordsToGeneratedData(recordsImportProperties)
    addRecordsToDelayedData(recordsImportProperties)
  }

  private def addRecordsToDataHolder(scenarioRecords: Map[DataGenerationScenario, Seq[Record]],
                                     dataHolder: TrieMap[String, EntityDataWithGenerationContext])
                                    (implicit entityMetadataGenerationProperties: EntityMetadataGenerationProperties): Unit = {

    val entityKey = getNormalizedEntityKey(entityMetadataGenerationProperties.entityMetadata)
    val idFieldName = entityMetadataGenerationProperties.entityMetadata.idFieldName

    val entityData: EntityDataWithGenerationContext = dataHolder.getOrElse(entityKey,
      EntityDataWithGenerationContext(entityMetadataGenerationProperties = entityMetadataGenerationProperties)
    )

    scenarioRecords.foreach { case (scenario, records) =>
      val scenarioRecordsMap = entityData.dataMap.getOrElse(scenario, TrieMap())
      records.foreach { record =>
        scenarioRecordsMap.addOne(record.get(idFieldName).toString, record)
      }
      entityData.dataMap.update(scenario, scenarioRecordsMap)
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

    val scenarioRecords: Map[DataGenerationScenario, Seq[Record]] = entityDataWithGenerationContext.dataMap
      .map { case (scenario, recordsMap) => (scenario, recordsMap.values.toSeq) }
      .toMap

    RecordsImportProperties(
      recordsToSend = None,
      recordsToDelay = Option(scenarioRecords),
      scenarioRecords = scenarioRecords,
      entityMetadataGenerationProperties = entityDataWithGenerationContext.entityMetadataGenerationProperties)
  }

  def getGeneratedData(key: String): Seq[Record] = {
    generatedData.get(getNormalizedEntityKey(key))
      .map(_.dataMap.values.flatMap(_.values).toSeq)
      .getOrElse(Nil)
  }

  def getGeneratedDataWithContext(key: String): Option[EntityDataWithGenerationContext] = {
    generatedData.get(getNormalizedEntityKey(key))
  }

  def getGeneratedDataSizeByKey(key: String): Int = {
    generatedData.get(getNormalizedEntityKey(key))
      .map(_.dataMap.values.flatMap(_.values).size)
      .getOrElse(0)
  }

  def getDelayedDataSizeByKey(key: String): Int = {
    delayedData.get(getNormalizedEntityKey(key))
      .map(_.dataMap.values.flatMap(_.values).size)
      .getOrElse(0)
  }

  //TODO maybe make sense to add scenario parameter
  def getValueForKeyByIndex(key: String, index: Int): Option[Record] = {
    generatedData.get(getNormalizedEntityKey(key))
      .map(_.dataMap.values.flatMap(_.values).toArray)
      .map(array => array(index))
  }

  //TODO maybe make sense to add scenario parameter
  def getDelayedValueForKeyByIndex(key: String, index: Int): Option[Record] = {
    delayedData.get(getNormalizedEntityKey(key))
      .map(_.dataMap.values.flatMap(_.values).toArray)
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
