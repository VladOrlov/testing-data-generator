package com.jvo.datagenerator.utils

import com.jvo.datagenerator.config.{Basic, DataGenerationScenario, DependencyDataDelay, EmptyStringValues, NullValues, WrongFormatValues}

object ScenarioUtils {

  def defineScenarioByName(scenarioName: String): DataGenerationScenario = {
    scenarioName.toUpperCase() match {
      case "BASIC" => Basic
      case "DEPENDENCYDATADELAY" => DependencyDataDelay
      case "EMPTYSTRINGVALUES" => EmptyStringValues
      case "NULLVALUES" => NullValues
      case "WRONGFORMATVALUES" => WrongFormatValues
      case _ => Basic
    }
  }

}
