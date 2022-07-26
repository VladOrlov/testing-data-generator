package com.jvo.datagenerator.services.keepers

import com.jvo.datagenerator.dto.entitydata.EntityMetadata
import com.jvo.datagenerator.services.EntityMetadataService
import com.jvo.datagenerator.utils.JsonObjectMapper

import java.io.File
import java.nio.file.Paths
import scala.collection.concurrent.TrieMap
import scala.io.Source

object EntitiesKeeper {

  val entities: TrieMap[String, EntityMetadata] = initEntities()

  def getAllEntities: Map[String, EntityMetadata] = {
    entities.toMap
  }

  def getEntity(entityName: String): Either[IllegalArgumentException, EntityMetadata] = {
    entities.get(entityName.toUpperCase)
      .map(Right.apply)
      .getOrElse(Left(new IllegalArgumentException(s"No Entity with name $entityName found!")))
  }

  def addEntity(entityMetadata: EntityMetadata): Unit = {
    entities.update(entityMetadata.name.toUpperCase, entityMetadata)
  }

  def removeEntity(entityName: String): Option[EntityMetadata] = {
    entities.remove(entityName.toUpperCase)
  }

  private def initEntities(): TrieMap[String, EntityMetadata] = {
    val (errors, entities) = loadEntities
      .map(JsonObjectMapper.parseToEntityRegistration)
      .map(_.flatMap(EntityMetadataService.mapToEntityMetadata))
      .partitionMap(identity)


    TrieMap.newBuilder
      .addAll(entities.map(entity => (entity.name.toUpperCase, entity)))
      .result()
  }

  private def loadEntities: Seq[String] = {
    val files: Seq[File] = getFilesInResourceFolder

    for {
      file: File <- files
    } yield readFile(file)
  }

  private def getFilesInResourceFolder: Seq[File] = {
    val url = getClass.getResource("/entities")
    val path = Paths.get(url.getPath)
    val file = path.toFile

    if (file.isDirectory) {
      file.listFiles().filter(_.getName.endsWith(".json")).toSeq
    } else {
      Nil
    }
  }

  def readFile(file: File): String = {
    val source = Source.fromFile(file)
    val entity = source.mkString
    source.close()
    entity
  }

}
