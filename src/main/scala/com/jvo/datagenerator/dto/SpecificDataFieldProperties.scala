package com.jvo.datagenerator.dto

import org.apache.avro.Schema

case class SpecificDataFieldProperties(field: Schema.Field, fieldRole: String)

object SpecificDataFieldProperties {

  implicit class RichSpecificDataField(specificDataField: SpecificDataFieldProperties) {

    def getFieldName: String = {
      specificDataField.field.name()
    }

  }
}
