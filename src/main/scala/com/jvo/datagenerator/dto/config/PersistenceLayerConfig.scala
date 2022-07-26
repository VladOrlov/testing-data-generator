package com.jvo.datagenerator.dto.config

case class EntityOperationsUrls(crudOperations: String, fetchWithFilter: String)
case class PersistenceLayerConfig(host: String, port: String, entityOperationsUrls: EntityOperationsUrls)
