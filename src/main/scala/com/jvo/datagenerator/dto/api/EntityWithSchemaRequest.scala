package com.jvo.datagenerator.dto.api


case class EntityWithSchemaRequest(name: String,
                                   role: Option[String],
                                   schemaJson: String,
                                   dependentFields: Option[Map[String, DependencyProperties]],
                                   specificDataFields: Option[Map[String, String]])

object EntityWithSchemaRequest {

  implicit class RichEntityWithSchemaRequest(entityWithSchemaRequest: EntityWithSchemaRequest) extends EntityRegistration {

    override def getDependentFields(): Option[Map[String, DependencyProperties]] = {
      entityWithSchemaRequest.dependentFields
    }

    override def getSpecificDataFields(): Option[Map[String, String]] = {
      entityWithSchemaRequest.specificDataFields
    }

    override def getName(): String = {
      entityWithSchemaRequest.getName()
    }

    override def getRole(): Option[String] = {
      entityWithSchemaRequest.role
    }
  }
}