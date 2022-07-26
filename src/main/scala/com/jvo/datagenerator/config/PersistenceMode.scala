package com.jvo.datagenerator.config

sealed trait PersistenceMode

case object InMemory extends PersistenceMode
case object InMemoryAndDatabase extends PersistenceMode
case object Database extends PersistenceMode