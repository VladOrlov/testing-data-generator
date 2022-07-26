package com.jvo.datagenerator.config

sealed trait DataGenerationMode
case object Batch extends DataGenerationMode
case object Stream extends DataGenerationMode
