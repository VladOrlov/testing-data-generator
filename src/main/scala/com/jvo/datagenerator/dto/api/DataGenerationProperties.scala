package com.jvo.datagenerator.dto.api

case class DataGenerationProperties(sinkType: Option[String],
                                    entityName: String,
                                    eventType: String,
                                    eventHubName: Option[String],
                                    sharedAccessKey: Option[String],
                                    sharedAccessKeyName: Option[String],
                                    dataFormat: Option[String],
                                    dataGenerationScenarios: Option[Seq[DataGenerationScenarioRequest]],
                                    recordsLimit: Option[Int])
