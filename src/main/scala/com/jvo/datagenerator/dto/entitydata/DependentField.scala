package com.jvo.datagenerator.dto.entitydata

import org.apache.avro.Schema

case class DependentField(field: Schema.Field, dependencyEntity: String, dependencyField: String)
