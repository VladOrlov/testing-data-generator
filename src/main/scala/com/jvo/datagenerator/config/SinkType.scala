package com.jvo.datagenerator.config

sealed trait SinkType
case object AzureFileStorage extends SinkType
case object AzureEventHub extends SinkType
