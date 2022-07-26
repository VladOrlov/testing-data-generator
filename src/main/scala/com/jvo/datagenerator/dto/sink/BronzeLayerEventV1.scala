package com.jvo.datagenerator.dto.sink

import java.sql.Timestamp

case class BronzeLayerEventV1(EntityName: String,
                              EventType: String,
                              MetadataVersion: Int,
                              ChangeDate: Timestamp,
                              ReceivedDate: Timestamp,
                              ProcessingDate: Timestamp,
                              BatchId: Int,
                              Body: String)
