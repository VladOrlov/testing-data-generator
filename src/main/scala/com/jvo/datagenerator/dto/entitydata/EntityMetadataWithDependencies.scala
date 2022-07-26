package com.jvo.datagenerator.dto.entitydata

import com.jvo.datagenerator.services.EntityMetadataService.FieldDependencyMetadata

case class EntityMetadataWithDependencies(entityMetadata: EntityMetadata, fieldsDependenciesMetadata: Set[FieldDependencyMetadata])
