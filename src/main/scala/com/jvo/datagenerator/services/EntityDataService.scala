package com.jvo.datagenerator.services

import com.jvo.datagenerator.config.DataGenerationScenario
import com.jvo.datagenerator.dto.RecordsImportProperties
import com.jvo.datagenerator.utils.AvroUtils
import org.apache.avro.generic.GenericData

object EntityDataService {

  def getDelayedRecords(recordsImportProperties: RecordsImportProperties): List[(String, Any)] = {
    recordsImportProperties.recordsToDelay
      .map(scenarioRecords => getRecordsAsMap(scenarioRecords))
      .getOrElse(Nil)
  }

  private def getRecordsAsMap(scenarioRecords: Map[DataGenerationScenario, Seq[GenericData.Record]]): List[(String, Any)] = {
    scenarioRecords.values
      .flatMap {
        records => records.flatMap(record => AvroUtils.convertRecordToMap(record))
      }.toList
  }

}
