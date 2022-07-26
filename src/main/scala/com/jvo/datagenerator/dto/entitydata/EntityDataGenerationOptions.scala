package com.jvo.datagenerator.dto.entitydata

import com.jvo.datagenerator.config.{DataGenerationMode, SinkType}

case class EntityDataGenerationOptions(sinkType: SinkType,
                                       env: String,
                                       targetPath: String,
                                       mode: DataGenerationMode,
                                       dataFormat: String,
                                       recordsLimit: Option[Int],
                                       delay: Option[Int])
