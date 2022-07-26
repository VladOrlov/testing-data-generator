package com.jvo.datagenerator.dto.entitydata

import org.apache.avro.generic.GenericData.Record

import scala.collection.concurrent.TrieMap

case class EntityDataWithGenerationContext(generationData: TrieMap[String, Record] = TrieMap(),
                                           entityMetadataGenerationProperties: EntityMetadataGenerationProperties)
