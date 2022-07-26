package com.jvo.datagenerator.dto

case class DataGenerationResult(started: Boolean, generatedEntities: Option[Map[String, String]])
