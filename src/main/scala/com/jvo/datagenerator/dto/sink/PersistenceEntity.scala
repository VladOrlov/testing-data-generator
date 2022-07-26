package com.jvo.datagenerator.dto.sink

case class PersistenceEntity(id: String,
                             entityName: String,
                             metadataVersion: String,
                             published: Boolean,
                             body: Map[String, Any])
