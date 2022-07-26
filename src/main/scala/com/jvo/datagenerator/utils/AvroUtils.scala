package com.jvo.datagenerator.utils

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

import java.io.{ByteArrayOutputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import org.apache.avro.file.{DataFileReader, DataFileWriter, SeekableByteArrayInput}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.reflect.{ReflectData, ReflectDatumReader, ReflectDatumWriter}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

object AvroUtils {

  val parser = new Parser()

  def parseSchema(schemaJson: String): Schema = {
    new Parser().parse(schemaJson)
  }

  def convertRecordToMap(record: Record): Map[String, Any] = {
    record.getSchema.getFields.asScala
      .map {
        field => (field.name(), record.get(field.name()))
      }
      .toMap
  }

  def writeToByteArray(records: Seq[Record], schema: Schema): Array[Byte] = {

    val outputStream = new ByteArrayOutputStream()
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(schema, outputStream)

    for (record <- records)
      dataFileWriter.append(record)

    dataFileWriter.flush()
    dataFileWriter.close()

    outputStream.toByteArray.map(byte => byte.toString)
    outputStream.toByteArray
  }

  def writeToStringArray(records: Seq[Record], schema: Schema): Array[String] = {

    val outputStream = new ByteArrayOutputStream()
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    dataFileWriter.create(schema, outputStream)

    for (record <- records)
      dataFileWriter.append(record)

    dataFileWriter.flush()
    dataFileWriter.close()

    outputStream.toByteArray.map(byte => byte.toString)
  }

  def readGenericData(bytes: Array[Byte], schema: Schema): List[org.apache.avro.generic.GenericRecord] = {

    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val inputStream = new SeekableByteArrayInput(bytes)
    val dataFileReader = new DataFileReader[GenericRecord](inputStream, datumReader)

    val list = dataFileReader.iterator().asScala.toList

    dataFileReader.close()

    list
  }

  def writeGenericBinary(records: Seq[Record],
                         schema: Schema): Array[Byte] = {

    val outputStream = new ByteArrayOutputStream()
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val encoder = EncoderFactory.get.binaryEncoder(outputStream, null)

    for (record <- records)
      datumWriter.write(record, encoder)

    encoder.flush()

    outputStream.toByteArray
  }

  def readGenericBinary(bytes: Array[Byte],
                        schema: Schema): List[org.apache.avro.generic.GenericRecord] = {

    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val inputStream = new SeekableByteArrayInput(bytes)
    val decoder = DecoderFactory.get.binaryDecoder(inputStream, null)

    val result = new collection.mutable.ListBuffer[org.apache.avro.generic.GenericRecord]
    while (!decoder.isEnd) {
      val item = datumReader.read(null, decoder)

      result += item
    }

    result.toList
  }

  def writeSpecificData[T](records: Seq[T])(implicit classTag: ClassTag[T]): Array[Byte] = {

    val outputStream = new ByteArrayOutputStream()
    val schema = ReflectData.get().getSchema(classTag.runtimeClass)

    val datumWriter = new ReflectDatumWriter[T](schema)
    val dataFileWriter = new DataFileWriter[T](datumWriter)
    dataFileWriter.create(schema, outputStream)

    for (record <- records)
      dataFileWriter.append(record)

    dataFileWriter.flush()
    dataFileWriter.close()

    outputStream.toByteArray
  }

  def readSpecificData[T](bytes: Array[Byte])(implicit classTag: ClassTag[T]): List[T] = {

    val schema = ReflectData.get().getSchema(classTag.runtimeClass)

    readSpecificData(bytes, schema)
  }

  def readSpecificData[T](bytes: Array[Byte], schema: Schema): List[T] = {

    val datumReader = new ReflectDatumReader[T](schema)
    val inputStream = new SeekableByteArrayInput(bytes)
    val dataFileReader = new DataFileReader[T](inputStream, datumReader)

    val list = dataFileReader.iterator().asScala.toList

    dataFileReader.close()

    list
  }

  def generateSchema[T]()(implicit classTag: ClassTag[T]): String = {
    val schema = ReflectData.get().getSchema(classTag.runtimeClass)

    schema.toString(true)
  }

  def writeSpecificBinary[T](value: T, schema: Schema): Array[Byte] = {
    val datumWriter = new ReflectDatumWriter[T](schema)
    val outputStream = new ByteArrayOutputStream()

    val encoder = EncoderFactory.get.binaryEncoder(outputStream, null)

    datumWriter.write(value, encoder)

    encoder.flush()

    outputStream.toByteArray
  }

  def readSpecificBinary[T](
                             bytes: Array[Byte],
                             readerSchema: Schema, // destination schema
                             writerSchema: Schema, // source schema
                             reuse: T): T = {
    val datumReader = new ReflectDatumReader[T](writerSchema, readerSchema)
    val inputStream = new SeekableByteArrayInput(bytes)

    val decoder = DecoderFactory.get.binaryDecoder(inputStream, null)

    datumReader.read(reuse, decoder)
  }

  def writeFile(fileName: String, bytes: Array[Byte]): Unit = {
    val file = new FileOutputStream(fileName)

    try {
      file.write(bytes)
    } finally {
      file.close()
    }
  }

  def readFile(fileName: String): Array[Byte] = {
    Files.readAllBytes(Paths.get(fileName))
  }
}
