package com.jvo.datagenerator.services


import com.jvo.datagenerator.config._
import com.jvo.datagenerator.dto.api.{DataGenerationProperties, DataGenerationRequest, DataGenerationScenarioRequest}
import com.jvo.datagenerator.dto._
import com.jvo.datagenerator.dto.entitydata._
import com.jvo.datagenerator.services.keepers.{DataKeeper, EntitiesKeeper, RolesKeeper}
import com.jvo.datagenerator.utils.{DataGeneratorUtils, ScenarioUtils}
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericData.Record

object DataGenerationService {

  implicit val orderingRatedEntityDependencies: Ordering[RatedEntityDependency] = Ordering.by[RatedEntityDependency, Int](_.ratio).reverse
  implicit val orderingByReferenceRatio: Ordering[ReferenceRatioEntities] = Ordering.by[ReferenceRatioEntities, Int](_.ratio)

  def generateData(dataGenerationRequest: DataGenerationRequest): Either[Set[IllegalArgumentException], Seq[(String, Seq[GenericData.Record])]] = {

    val maybeRecords = getEntityMetadataGenerationProperties(dataGenerationRequest)
      .map { entityMetadataGenerationProperties: Set[EntityMetadataGenerationProperties] =>

        val ratedDependencies: Seq[RatedEntityDependency] = getRatedEntityDependencies(entityMetadataGenerationProperties)
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

    DataKeeper.getGeneratedDataWithContext(entityFields.entityName)
      .map { dataWithGenerationContext: EntityDataWithGenerationContext =>
        val records = dataWithGenerationContext.generationData.values
          .takeRight((dataWithGenerationContext.generationData.size * 0.1).toInt)
          .map(record => DataGeneratorUtils.updateRecord(record, dataWithGenerationContext.entityMetadataGenerationProperties.entityMetadata))
          .toSeq

        RecordsImportProperties(records, dataWithGenerationContext.entityMetadataGenerationProperties)
      }
  }

  private def processEntityMetadataSequence(ratedDependencies: Seq[ReferenceRatioEntities])
                                           (implicit entityGenerationPropertiesMap: Map[String, EntityGenerationProperties]): Seq[(String, Seq[GenericData.Record])] = {
    ratedDependencies
      .flatMap { dependencyEntities: ReferenceRatioEntities =>
        dependencyEntities.ratedEntityDependencies
          .sorted
          .map(_.entityMetadata)
          .flatMap { implicit entity: EntityMetadata => generateAndPushData }
      }
  }

  private def generateAndPushData(implicit entityGenerationPropertiesMap: Map[String, EntityGenerationProperties],
                                  entityMetadata: EntityMetadata): Option[(String, Seq[GenericData.Record])] = {

    entityGenerationPropertiesMap.get(entityMetadata.name)
      .map { entityGenerationProperties: EntityGenerationProperties =>
        val records: Seq[Record] = generateRecordsForEntityMetadata(entityMetadata, entityGenerationProperties.recordsLimit)
        val entityMetadataGenerationProperties = EntityMetadataGenerationProperties(entityMetadata, entityGenerationProperties)

        val recordsImportProperties: RecordsImportProperties = getRecordImportProperties(records, entityMetadataGenerationProperties)

        persistGeneratedData(recordsImportProperties)

        DataUploader.pushToAzureEventHub(recordsImportProperties)
        (entityMetadata.name, records)
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

  private def getRecordImportProperties(records: Seq[Record],
                                        entityMetadataGenerationProperties: EntityMetadataGenerationProperties): RecordsImportProperties = {

    val (recordsToSend, recordsDelayed) =
      if (entityMetadataGenerationProperties.hasDataDelayScenario) {
        val delayRecordsNumber: Int = getDelayedRecordsNumber(records)
        (Some(records.dropRight(delayRecordsNumber)), Some(records.takeRight(delayRecordsNumber)))
      } else {
        (Some(records), None)
      }

    RecordsImportProperties(recordsToSend, recordsDelayed, entityMetadataGenerationProperties)
  }

  private def getEntityGenerationPropertiesMap(implicit entityMetadataGenerationProperties: Set[EntityMetadataGenerationProperties]) = {

    entityMetadataGenerationProperties
      .map { generationProperties =>
        (generationProperties.entityMetadata.name, generationProperties.entityGenerationProperties)
      }
      .toMap
  }

  private def getDelayedRecordsNumber(records: Seq[GenericData.Record]): Int = {
    val recordsNumber = (records.size * 0.1).toInt

    if (recordsNumber == 0) 1 else recordsNumber
  }

  private def getDataGenerationScenario(property: DataGenerationProperties)
                                       (implicit dataGenerationRequest: DataGenerationRequest): Seq[EntityGenerationScenarioProperties] = {
    property.dataGenerationScenarios
      .map(dataGenerationScenarios => dataGenerationScenarios.map(mapToEntityGenerationScenario))
      .getOrElse(getEntityGenerationScenariosFromRoot(property, dataGenerationRequest))
  }

  private def getEntityGenerationScenariosFromRoot(property: DataGenerationProperties,
                                                   dataGenerationRequest: DataGenerationRequest): Seq[EntityGenerationScenarioProperties] = {

    val scenarios: Seq[EntityGenerationScenarioProperties] = dataGenerationRequest.dataGenerationScenarios
      .filter(scenarioRequest => entityHasDataGenerationScenario(scenarioRequest, property))
      .map(scenarioRequest => mapToEntityGenerationScenario(scenarioRequest))

    if (scenarios.isEmpty)
      Seq(EntityGenerationScenarioProperties(dataGenerationScenario = Basic, entityFields = None))
    else
      scenarios
  }

  private def mapToEntityGenerationScenario(scenarioRequest: DataGenerationScenarioRequest): EntityGenerationScenarioProperties = {
    EntityGenerationScenarioProperties(
      dataGenerationScenario = ScenarioUtils.defineScenarioByName(scenarioRequest.scenarioName),
      entityFields = scenarioRequest.entityFields)
  }

  private def entityHasDataGenerationScenario(scenarioRequest: DataGenerationScenarioRequest, property: DataGenerationProperties): Boolean = {
    scenarioRequest.entityFields.exists(fields => if (fields.entityName == property.entityName) true else false)
  }

  private def mapToEntityGenerationProperty(generationProperties: DataGenerationProperties)
                                           (implicit dataGenerationRequest: DataGenerationRequest): EntityGenerationProperties = {
    EntityGenerationProperties(
      server = dataGenerationRequest.server,
      sinkType = generationProperties.sinkType.getOrElse(dataGenerationRequest.sinkType),
      sinkSchema = dataGenerationRequest.sinkSchema,
      dataFormat = generationProperties.dataFormat.getOrElse(dataGenerationRequest.dataFormat),
      mode = dataGenerationRequest.mode,
      entityGenerationScenarioProperties = getDataGenerationScenario(generationProperties),
      recordsLimit = generationProperties.recordsLimit.getOrElse(dataGenerationRequest.recordsLimit.getOrElse(10)),
      recordsRatePerSecond = dataGenerationRequest.recordsRatePerSecond,
      entityName = generationProperties.entityName,
      eventType = generationProperties.eventType,
      eventHubName = generationProperties.eventHubName.orElse(dataGenerationRequest.eventHubName).orNull,
      sharedAccessKey = generationProperties.sharedAccessKey.orElse(dataGenerationRequest.sharedAccessKey).orNull,
      sharedAccessKeyName = generationProperties.sharedAccessKeyName.orElse(dataGenerationRequest.sharedAccessKeyName).orNull
    )
  }

  private def getRatedEntityDependencies(entityMetadataGenerationProperties: Set[EntityMetadataGenerationProperties]) = {
    entityMetadataGenerationProperties
      .flatMap((emProps: EntityMetadataGenerationProperties) => getGenerationDependenciesForEntity(emProps.entityMetadata))
      .toSeq
      .sorted
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

  private def updateGenerationPropertiesIfContainsDelayScenario(properties: Set[EntityMetadataGenerationProperties]) = {

    val propertiesWithDependencyDataDelayScenario: Set[String] = properties
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

    properties
      .filter(generationProperties => propertiesWithDependencyDataDelayScenario.contains(generationProperties.entityMetadata.name))
      .map(addDataDelayScenarioToGenerationProperties) ++ properties
      .filterNot(generationProperties => propertiesWithDependencyDataDelayScenario.contains(generationProperties.entityMetadata.name))
  }

  private def addDataDelayScenarioToGenerationProperties(metadataGenerationProperties: EntityMetadataGenerationProperties) = {

    metadataGenerationProperties.copy(
      entityGenerationProperties = metadataGenerationProperties.entityGenerationProperties.copy(
        entityGenerationScenarioProperties = dropBasicAndAddDataDelayScenario(metadataGenerationProperties)))
  }

  private def dropBasicAndAddDataDelayScenario(metadataGenerationProperties: EntityMetadataGenerationProperties) = {
    metadataGenerationProperties.entityGenerationProperties.entityGenerationScenarioProperties
      .filterNot(_.dataGenerationScenario == Basic) :+ EntityGenerationScenarioProperties(DataDelay)
  }

  private def generateRecordsForEntityMetadata(entityMetadata: EntityMetadata, recordsNumber: Int) = {

    val mapToRecordFunction: EntityMetadata => GenericData.Record = RolesKeeper.getMapToRecordFunction(entityMetadata.role)

    val records = (1 to recordsNumber)
      .map { _ =>
        mapToRecordFunction.apply(entityMetadata)
      }
    records
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

  private def getGenerationDependenciesForEntity(entityMetadata: EntityMetadata): Set[RatedEntityDependency] = {
    Set(RatedEntityDependency(entityMetadata, entityMetadata.dependentFields.size)) ++ entityMetadata.dependentFields.flatMap(getDependency)
  }

  private def getDependency(dependentField: DependentField): Set[RatedEntityDependency] = {
    EntitiesKeeper.getEntity(dependentField.dependencyEntity) match {
      case Right(entityMetadata: EntityMetadata) =>
        if (entityMetadata.dependentFields.isEmpty)
          Set(RatedEntityDependency(entityMetadata, 0))
        else
          getGenerationDependenciesForEntity(entityMetadata)
      case Left(ex) => Set()
    }
  }

}
