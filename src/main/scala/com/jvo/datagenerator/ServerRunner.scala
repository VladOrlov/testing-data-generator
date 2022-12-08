package com.jvo.datagenerator

import com.jvo.datagenerator.api._
import com.jvo.datagenerator.dto.config.ApplicationParameters
import com.jvo.datagenerator.services.PersistenceClient
import wvlet.airframe.http.Router
import wvlet.airframe.http.finagle._

object ServerRunner {

  // Define API routes. This will read all @Endpoint annotations in MyApi
  // You can add more routes by using `.add[X]` method.

  def main(args: Array[String]): Unit = {

    val applicationParameters: ApplicationParameters = parseArgs(args)
      .getOrElse(throw new IllegalArgumentException("Provided application parameters not valid!"))

    PersistenceClient.init(applicationParameters)

    val router = Router
      .add[SchemaController]
      .add[EntityMetadataController]
      .add[DataGenerationController]
      .add[EntityDataController]
      .add[RolesController]

    //Write any other initialization BEFORE Finagle server init
    Finagle.server
      .withPort(8080)
      .withRouter(router)
      .start { server =>
        // Finagle http server will start here
        // To keep running the server, run `server.waitServerTermination`:
        server.waitServerTermination
      }

    // The server will terminate here
  }

  private def parseArgs(args: Array[String]): Option[ApplicationParameters] = {
    //    CommandLineParser
    val parser = new scopt.OptionParser[ApplicationParameters]("scopt") {
      head("scopt", "3.x")

      opt[String]("env").optional().action { (x, c) =>
        c.copy(env = x)
      }
      opt[String]("config-path").optional().action { (x, c) =>
        c.copy(configPath = x)
      }
      opt[String]("persistence-server").optional().action { (x, c) =>
        c.copy(configPath = x)
      }
    }

    parser.parse(args, ApplicationParameters())
  }
  //  val r = requests.get("http://localhost:8090/v1/generated-data/entities/filter?entityName=member&published=false")
  //  println(r.text())


}
