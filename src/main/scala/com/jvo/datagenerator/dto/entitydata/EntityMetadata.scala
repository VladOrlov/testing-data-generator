package com.jvo.datagenerator.dto.entitydata

import com.jvo.datagenerator.dto.SpecificDataFieldProperties
import com.jvo.datagenerator.services.keepers.RolesKeeper.GenericRole
import org.apache.avro.Schema

case class EntityMetadata(name: String,
                          role: String = GenericRole,
                          schema: Schema,
                          idFieldName: String = "id",
                          dependentFields: Set[DependentField],
                          specificDataFields: Set[SpecificDataFieldProperties])
