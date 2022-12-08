package com.jvo.datagenerator.dto.entitydata

import org.apache.avro.Schema

case class DependentFieldProperties(field: Schema.Field, dependencyEntity: String, dependencyField: String)
