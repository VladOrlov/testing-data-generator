package com.jvo.datagenerator.dto.entitydata

import com.jvo.datagenerator.config.DataGenerationScenario

case class EntityGenerationScenarioProperties(dataGenerationScenario: DataGenerationScenario,
                                              entityFields: Option[EntityFields] = None)
