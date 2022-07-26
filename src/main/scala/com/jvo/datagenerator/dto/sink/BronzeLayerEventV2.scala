package com.jvo.datagenerator.dto.sink

import java.sql.Timestamp

case class BronzeLayerEventV2(EntityId: String,
                              EntityName: String,
                              EntityVersion: Int,
                              EventType: String,
                              ProcessingDateTime: Timestamp,
                              BatchId: Int,
                              Data: String)
