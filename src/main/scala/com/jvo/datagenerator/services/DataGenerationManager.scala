package com.jvo.datagenerator.services


import com.jvo.datagenerator.config._
import com.jvo.datagenerator.dto.api.{DataGenerationProperties, DataGenerationRequest, DataGenerationScenarioRequest}
import com.jvo.datagenerator.dto._
import com.jvo.datagenerator.dto.entitydata._
import com.jvo.datagenerator.services.keepers.{DataKeeper, EntitiesKeeper, RolesKeeper}
import com.jvo.datagenerator.utils.{DataGeneratorUtils, ScenarioUtils}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record

import scala.Option

object DataGenerationManager {

  implicit val orderingRatedEntityDependencies: Ordering[RatedEntityDependency] = Ordering.by[RatedEntityDependency, Int](_.ratio).reverse
  implicit val orderingByReferenceRatio: Ordering[ReferenceRatioEntities] = Ordering.by[ReferenceRatioEntities, Int](_.ratio)

  def generateData(dataGenerationRequest: DataGenerationRequest): Either[Set[IllegalArgumentException], Seq[(String, Map[DataGenerationScenario, Seq[Record]])]] = {

    val maybeRecords = getEntityMetadataGenerationProperties(dataGenerationRequest)
      .map { entityMetadataGenerationProperties: Set[EntityMetadataGenerationProperties] =>

        val ratedDependencies: Seq[RatedEntityDependency] = EntityDependencyBuilder.getRatedEntityDependencies(entityMetadataGenerationProperties)
        val referenceRatedDependencies: Seq[ReferenceRatioEntities] = getReferenceRatioDependenciesForEntity(ratedDependencies)
        implicit val entityGenerationPropertiesMap: Map[String, EntityGenerationProperties] = getEntityGenerationPropertiesMap(entityMetadataGenerationProperties)

        processEntityMetadataSequence(referenceRatedDependencies)

      }
    maybeRecords
  }

  def pushDelayedData(): Map[String, Int] = {

    val delayedDataMap = DataKeeper.getAllDelayedDataMap

    delayedDataMap.map {
      case (entityName, recordsImportProperties) =>
        val updatedRecordsImportProperties = setDelayedRecordsToSend(recordsImportProperties)

        DataUploader.pushToAzureEventHub(updatedRecordsImportProperties)
        persistGeneratedData(updatedRecordsImportProperties)
        DataKeeper.removeDelayedEntityData(recordsImportProperties.getEntityMetadata)

        (entityName, recordsImportProperties.recordsToDelay.map(_.size).getOrElse(0))
    }
  }

  private def setDelayedRecordsToSend(recordsImportProperties: RecordsImportProperties) = {
    recordsImportProperties.copy(
      recordsToSend = recordsImportProperties.recordsToDelay,
      recordsToDelay = None)
  }

  def updateGeneratedData(entitiesToUpdate: List[EntityFields]): Seq[RecordsImportProperties] = {

    entitiesToUpdate
      .flatMap((entityFields: EntityFields) => updateValuesForEntity(entityFields))
      .map { recordsImportProperties =>

        persistGeneratedData(recordsImportProperties)
        DataUploader.pushToAzureEventHub(recordsImportProperties)

        recordsImportProperties
      }
  }

  private def updateValuesForEntity(entityFields: EntityFields): Option[RecordsImportProperties] = {

    //    DataKeeper.getGeneratedDataWithContext(entityFields.entityName)
    //      .map { dataWithGenerationContext: EntityDataWithGenerationContext =>
    //        val records = dataWithGenerationContext.generationData.values
    //          .takeRight((dataWithGenerationContext.generationData. * 0.1).toInt)
    //          .map(record => DataGeneratorUtils.updateRecord(record, dataWithGenerationContext.entityMetadataGenerationProperties.entityMetadata))
    //          .toSeq
    //
    //        RecordsImportProperties(records, dataWithGenerationContext.entityMetadataGenerationProperties)
    //      }

    None
  }

  private def processEntityMetadataSequence(ratedDependencies: Seq[ReferenceRatioEntities])
                                           (implicit entityGenerationPropertiesMap: Map[String, EntityGenerationProperties]): Seq[(String, Map[DataGenerationScenario, Seq[Record]])] = {
    ratedDependencies
      .flatMap { dependencyEntities: ReferenceRatioEntities =>
        dependencyEntities.ratedEntityDependencies
          .sorted
          .map(_.entityMetadata)
          .flatMap { implicit entity: EntityMetadata => generateAndPushData }
      }
  }

  private def generateAndPushData(implicit entityGenerationPropertiesMap: Map[String, EntityGenerationProperties],
                                  entityMetadata: EntityMetadata): Option[(String, Map[DataGenerationScenario, Seq[Record]])] = {

    entityGenerationPropertiesMap.get(entityMetadata.name)
      .map { entityGenerationProperties: EntityGenerationProperties =>

        val scenarioRecords: Map[DataGenerationScenario, Seq[Record]] = generateRecordsForEntityMetadata(entityMetadata, entityGenerationProperties)
        val entityMetadataGenerationProperties = EntityMetadataGenerationProperties(entityMetadata, entityGenerationProperties)
        val recordsImportProperties: RecordsImportProperties = getRecordImportProperties(scenarioRecords, entityMetadataGenerationProperties)

        persistGeneratedData(recordsImportProperties)

        //DataUploader.pushToAzureEventHub(recordsImportProperties)
        (entityMetadata.name, scenarioRecords)
      }
  }

  private def persistGeneratedData(recordsImportProperties: RecordsImportProperties): Unit = {

    recordsImportProperties.getDataPersistenceMode match {
      case InMemory =>
        DataKeeper.addRecords(recordsImportProperties)
      case InMemoryAndDatabase =>
        DataKeeper.addRecords(recordsImportProperties)
        PersistenceClient.saveEntityRecords(recordsImportProperties)
      case Database =>
        PersistenceClient.saveEntityRecords(recordsImportProperties)
    }
  }

  private def getRecordImportProperties(scenarioRecords: Map[DataGenerationScenario, Seq[Record]],
                                        entityMetadataGenerationProperties: EntityMetadataGenerationProperties): RecordsImportProperties = {

    RecordsImportProperties(
      recordsToSend = Option(scenarioRecords.removed(DependencyDataDelay)),
      recordsToDelay = scenarioRecords.get(DependencyDataDelay).map(records => Map(DependencyDataDelay -> records)),
      scenarioRecords = scenarioRecords,
      entityMetadataGenerationProperties = entityMetadataGenerationProperties
    )
  }

  private def getEntityGenerationPropertiesMap(implicit entityMetadataGenerationProperties: Set[EntityMetadataGenerationProperties]) = {

    entityMetadataGenerationProperties
      .map { generationProperties =>
        (generationProperties.entityMetadata.name, generationProperties.entityGenerationProperties)
      }
      .toMap
  }

  private def getDataGenerationScenario(generationProperties: DataGenerationProperties, recordsLimit: Int)
                                       (implicit dataGenerationRequest: DataGenerationRequest): Seq[EntityGenerationScenarioProperties] = {

    generationProperties.dataGenerationScenarios
      .map(dataGenerationScenarios => dataGenerationScenarios.map(scenario => mapToEntityGenerationScenario(scenario, recordsLimit)))
      .getOrElse(getEntityGenerationScenariosFromRoot(generationProperties, dataGenerationRequest))
  }

  private def getEntityGenerationScenariosFromRoot(generationProperties: DataGenerationProperties,
                                                   dataGenerationRequest: DataGenerationRequest): Seq[EntityGenerationScenarioProperties] = {

    val recordsLimit = generationProperties.recordsLimit.getOrElse(dataGenerationRequest.recordsLimit.getOrElse(10))

    val scenarios: Seq[EntityGenerationScenarioProperties] = dataGenerationRequest.dataGenerationScenarios
      .filter(scenarioRequest => entityHasDataGenerationScenario(scenarioRequest, generationProperties))
      .map(scenarioRequest => mapToEntityGenerationScenario(scenarioRequest, recordsLimit))

    if (scenarios.isEmpty) {
      Seq(EntityGenerationScenarioProperties(dataGenerationScenario = Basic, recordsNumber = recordsLimit))
    } else {
      val basicRecordsNumber = recordsLimit - scenarios.map(_.recordsNumber).sum
      if (basicRecordsNumber < 0)
        scenarios
      else
        scenarios :+ EntityGenerationScenarioProperties(dataGenerationScenario = Basic, recordsNumber = basicRecordsNumber)
    }
  }

  private def mapToEntityGenerationScenario(scenarioRequest: DataGenerationScenarioRequest, recordsLimit: Int): EntityGenerationScenarioProperties = {
    EntityGenerationScenarioProperties(
      dataGenerationScenario = ScenarioUtils.defineScenarioByName(scenarioRequest.scenarioName),
      entityFields = scenarioRequest.entityFields,
      recordsNumber = scenarioRequest.recordsNumber.getOrElse(recordsLimit * 10 / 100))
  }

  private def entityHasDataGenerationScenario(scenarioRequest: DataGenerationScenarioRequest, property: DataGenerationProperties): Boolean = {
    scenarioRequest.entityFields.exists(fields => if (fields.entityName == property.entityName) true else false)
  }

  private def mapToEntityGenerationProperty(generationProperties: DataGenerationProperties)
                                           (implicit dataGenerationRequest: DataGenerationRequest): EntityGenerationProperties = {

    val dataGenerationScenario = getDataGenerationScenario(generationProperties,
      generationProperties.recordsLimit.getOrElse(dataGenerationRequest.recordsLimit.getOrElse(10)))

    println(dataGenerationScenario)
    EntityGenerationProperties(
      server = dataGenerationRequest.server,
      sinkType = generationProperties.sinkType.getOrElse(dataGenerationRequest.sinkType),
      sinkSchema = dataGenerationRequest.sinkSchema,
      dataFormat = generationProperties.dataFormat.getOrElse(dataGenerationRequest.dataFormat),
      mode = dataGenerationRequest.mode,
      entityGenerationScenarioProperties = dataGenerationScenario,
      recordsLimit = generationProperties.recordsLimit.getOrElse(dataGenerationRequest.recordsLimit.getOrElse(10)),
      recordsRatePerSecond = dataGenerationRequest.recordsRatePerSecond,
      entityName = generationProperties.entityName,
      eventType = generationProperties.eventType,
      eventHubName = generationProperties.eventHubName.orElse(dataGenerationRequest.eventHubName).orNull,
      sharedAccessKey = generationProperties.sharedAccessKey.orElse(dataGenerationRequest.sharedAccessKey).orNull,
      sharedAccessKeyName = generationProperties.sharedAccessKeyName.orElse(dataGenerationRequest.sharedAccessKeyName).orNull
    )
  }


  private def getEntityMetadataGenerationProperties(implicit dataGenerationRequest: DataGenerationRequest) = {

    val (errors, properties: Set[EntityMetadataGenerationProperties]) =
      dataGenerationRequest.entitiesGenerationProperties.toSet
        .map { generationProperties: DataGenerationProperties =>
          EntitiesKeeper.getEntity(generationProperties.entityName)
            .map(entity => EntityMetadataGenerationProperties(entity, mapToEntityGenerationProperty(generationProperties)))
        }
        .partitionMap(identity)

    if (errors.isEmpty) {
      Right(updateGenerationPropertiesIfContainsDelayScenario(properties))
    } else {
      Left(errors)
    }
  }

  private def updateGenerationPropertiesIfContainsDelayScenario(properties: Set[EntityMetadataGenerationProperties]): Set[EntityMetadataGenerationProperties] = {

    val propertiesWithDependencyDataDelayScenario: Set[String] = getPropertiesWithDependencyDataDelayScenario(properties)

    val propertiesWithDelayedScenarios = properties
      .filter(generationProperties => propertiesWithDependencyDataDelayScenario.contains(generationProperties.entityMetadata.name))
      .map(addDataDelayScenarioToGenerationProperties)

    propertiesWithDelayedScenarios ++ properties
      .filterNot(generationProperties => propertiesWithDependencyDataDelayScenario.contains(generationProperties.entityMetadata.name))
  }

  private def getPropertiesWithDependencyDataDelayScenario(properties: Set[EntityMetadataGenerationProperties]): Set[String] = {
    properties
      .flatMap { (props: EntityMetadataGenerationProperties) =>
        props.entityGenerationProperties.entityGenerationScenarioProperties
          .find(_.dataGenerationScenario == DependencyDataDelay)
          .map(scenario => (props, scenario))
      }
      .flatMap { tuple =>
        tuple._2.entityFields
          .map(entityFields => Set(entityFields.entityName))
          .getOrElse(tuple._1.entityMetadata.dependentFields.map(_.dependencyEntity))
      }
  }

  private def addDataDelayScenarioToGenerationProperties(metadataGenerationProperties: EntityMetadataGenerationProperties) = {

    metadataGenerationProperties.getEntityGenerationScenarioProperties
      .find(_.dataGenerationScenario == DataDelay) match {
      case Some(_) =>
        metadataGenerationProperties
      case None =>
        metadataGenerationProperties.copy(
          entityGenerationProperties = metadataGenerationProperties.entityGenerationProperties.copy(
            entityGenerationScenarioProperties = addDataDelayScenario(metadataGenerationProperties)))
    }
  }

  private def addDataDelayScenario(metadataGenerationProperties: EntityMetadataGenerationProperties): Seq[EntityGenerationScenarioProperties] = {

    val dataDelayRecordsNumber = metadataGenerationProperties.getRecordsSizeLimit * 10 / 100
    val basicScenarioRecords = metadataGenerationProperties.getRecordsSizeLimit - dataDelayRecordsNumber

    val updatedScenarios =
      if (basicScenarioRecords <= 0) {
        getNonBasicScenarios(metadataGenerationProperties)
      } else {
        metadataGenerationProperties.getEntityGenerationScenarioProperties
          .find(_.dataGenerationScenario == Basic)
          .map(basicScenario => getNonBasicScenarios(metadataGenerationProperties) :+
            basicScenario.copy(recordsNumber = basicScenarioRecords))
          .getOrElse(metadataGenerationProperties.getEntityGenerationScenarioProperties)
      }

    val dataDelayScenario = EntityGenerationScenarioProperties(dataGenerationScenario = DataDelay, recordsNumber = dataDelayRecordsNumber)

    updatedScenarios :+ dataDelayScenario
  }

  private def getNonBasicScenarios(metadataGenerationProperties: EntityMetadataGenerationProperties): Seq[EntityGenerationScenarioProperties] = {
    metadataGenerationProperties.getEntityGenerationScenarioProperties
      .filter(_.dataGenerationScenario != Basic)
  }

  private def generateRecordsForEntityMetadata(entityMetadata: EntityMetadata,
                                               entityGenerationProperties: EntityGenerationProperties): Map[DataGenerationScenario, Seq[Record]] = {

    val mapToRecordFunction: (EntityMetadata, EntityGenerationScenarioProperties) => GenericData.Record =
      RolesKeeper.getMapToRecordFunction(entityMetadata.role)

    val records =
      for {
        entityGenerationScenarioProperty <- entityGenerationProperties.entityGenerationScenarioProperties
        _ <- 1 to entityGenerationScenarioProperty.recordsNumber
      } yield {
        (entityGenerationScenarioProperty.dataGenerationScenario,
          mapToRecordFunction.apply(entityMetadata, entityGenerationScenarioProperty))
      }

    records.groupMap(_._1)(_._2)

  }

  private def getReferenceRatioDependenciesForEntity(entityWithDependencies: Seq[RatedEntityDependency]): Seq[ReferenceRatioEntities] = {

    val entityReferencesRatio: Map[String, Int] = entityWithDependencies
      .flatMap(_.entityMetadata.dependentFields.map(_.dependencyEntity))
      .map(entityName => (entityName, 1))
      .groupMapReduce(_._1)(_._2)(_ + _)

    val ratedDependencies = entityWithDependencies
      .groupMap(_.ratio)((r: RatedEntityDependency) => r.copy(ratio = entityReferencesRatio.getOrElse(r.entityMetadata.name, 0)))
      .map {
        case (referenceRatio, dependencyEntities) => ReferenceRatioEntities(referenceRatio, dependencyEntities.sorted)
      }
      .toSeq

    ratedDependencies.sorted
  }

}
