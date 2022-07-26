package com.jvo.datagenerator.api

import com.jvo.datagenerator.services.keepers.RolesKeeper
import wvlet.airframe.http.{Endpoint, HttpMethod}

@Endpoint(path = "/v1/roles")
trait RolesController {

  @Endpoint(method = HttpMethod.GET, path = "")
  def getRoles: Seq[(String, Seq[String])] = {
    RolesKeeper.fieldRoles.toSeq
      .map(p => (p._1, p._2.toSeq))
  }
}
