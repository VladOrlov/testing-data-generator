package com.jvo.datagenerator.dto.api

case class EntityRegistrationRequest(name: String,
                                     role: Option[String],
                                     schemaName: String,
                                     dependentFields: Option[Map[String, DependencyProperties]],
                                     specificDataFields: Option[Map[String, String]])

object EntityRegistrationRequest {

  implicit class RichEntityRegistrationRequest(entityRegistrationRequest: EntityRegistrationRequest) extends EntityRegistration {

    override def getDependentFields(): Option[Map[String, DependencyProperties]] = {
      entityRegistrationRequest.dependentFields
    }

    override def getSpecificDataFields(): Option[Map[String, String]] = {
      entityRegistrationRequest.specificDataFields
    }

    override def getName(): String = {
      entityRegistrationRequest.name
    }

    override def getRole(): Option[String] = {
      entityRegistrationRequest.role
    }
  }
}