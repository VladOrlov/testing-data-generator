package com.jvo.datagenerator.services

import com.jvo.datagenerator.dto.entitydata.{DependentFieldProperties, EntityMetadata, EntityMetadataGenerationProperties, RatedEntityDependency, ReferenceRatioEntities}
import com.jvo.datagenerator.services.keepers.EntitiesKeeper

object EntityDependencyBuilder {

  implicit val orderingRatedEntityDependencies: Ordering[RatedEntityDependency] = Ordering.by[RatedEntityDependency, Int](_.ratio).reverse
  implicit val orderingByReferenceRatio: Ordering[ReferenceRatioEntities] = Ordering.by[ReferenceRatioEntities, Int](_.ratio)

  def getRatedEntityDependencies(entityMetadataGenerationProperties: Set[EntityMetadataGenerationProperties]): Seq[RatedEntityDependency] = {
    entityMetadataGenerationProperties
      .flatMap((emProps: EntityMetadataGenerationProperties) => getRatedEntityDependencies(emProps.entityMetadata))
      .toSeq
      .sorted
  }

  private def getRatedEntityDependencies(entityMetadata: EntityMetadata): Set[RatedEntityDependency] = {
    Set(RatedEntityDependency(entityMetadata, entityMetadata.dependentFields.size)) ++ entityMetadata.dependentFields.flatMap(getDependency)
  }

  private def getDependency(dependentField: DependentFieldProperties): Set[RatedEntityDependency] = {
    EntitiesKeeper.getEntity(dependentField.dependencyEntity) match {
      case Right(entityMetadata: EntityMetadata) =>
        if (entityMetadata.dependentFields.isEmpty)
          Set(RatedEntityDependency(entityMetadata, 0))
        else
          getRatedEntityDependencies(entityMetadata)
      case Left(ex) => Set()
    }
  }
}
