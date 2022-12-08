package com.jvo.datagenerator.dto.api

import com.jvo.datagenerator.dto.entitydata.EntityFields

case class DataGenerationScenarioRequest(scenarioName: String,
                                         entityFields: Option[EntityFields] = None,
                                         recordsNumber: Option[Int] = None)
