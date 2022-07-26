package com.jvo.datagenerator.dto.config

case class ApplicationParameters(env: String = "default",
                                 configPath: String = "./src/main/resources/config",
                                 persistenceServer: String = "localhost:8090")
