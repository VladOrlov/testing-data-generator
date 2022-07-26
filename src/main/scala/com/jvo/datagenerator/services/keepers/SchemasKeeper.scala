package com.jvo.datagenerator.services.keepers

import com.jvo.datagenerator.utils.AvroUtils
import org.apache.avro.Schema

import java.io.File
import java.nio.file.Paths
import scala.collection.concurrent.TrieMap
import scala.io.Source

object SchemasKeeper {

  private[this] val schemas: TrieMap[String, Schema] = initSchemas()

  def initSchemas(): TrieMap[String, Schema] = {
    TrieMap.newBuilder
      .addAll(loadSchemas
        .map(AvroUtils.parseSchema)
        .map(schema => (schema.getFullName, schema)))
      .result()
  }

  def getAllSchemas: Map[String, Schema] = {
    schemas.toMap
  }

  def getSchema(schemaName: String): Either[IllegalArgumentException, Schema] = {
    schemas.get(schemaName)
      .map(Right.apply)
      .getOrElse(Left(new IllegalArgumentException(s"No schema found for key $schemaName")))
  }

  def addSchema(schema: Schema): Unit = {
    schemas.update(schema.getFullName, schema)
  }

  def removeSchema(schemaName: String): Unit = {
    schemas.remove(schemaName)
  }

  private def loadSchemas: Seq[String] = {
    val files: Seq[File] = getFilesInResourceFolder

    for {
      file: File <- files
    } yield readFile(file)
  }

  private def getFilesInResourceFolder: Seq[File] = {
    val url = getClass.getResource("/schemas")
    val path = Paths.get(url.getPath)
    val file = path.toFile

    if (file.isDirectory) {
      file.listFiles().filter(_.getName.endsWith(".avsc")).toSeq
    } else {
      Nil
    }
  }

  def readFile(file: File): String = {
    val source = Source.fromFile(file)
    val schema = source.mkString
    source.close()
    schema
  }

}
