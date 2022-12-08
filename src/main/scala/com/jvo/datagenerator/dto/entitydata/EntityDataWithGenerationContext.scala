package com.jvo.datagenerator.dto.entitydata

import com.jvo.datagenerator.config.DataGenerationScenario
import org.apache.avro.generic.GenericData.Record
import scala.collection.concurrent.TrieMap

case class EntityDataWithGenerationContext(dataMap: TrieMap[DataGenerationScenario, TrieMap[String, Record]] = TrieMap(),
                                           entityMetadataGenerationProperties: EntityMetadataGenerationProperties)

