package com.jvo.datagenerator.dto.api

trait EntityRegistration {

  def getDependentFields(): Option[Map[String, DependencyProperties]]

  def getSpecificDataFields(): Option[Map[String, String]]

  def getName(): String

  def getRole(): Option[String]

}
