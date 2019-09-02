package aml.sql.utils

import amf.core.AMF
import amf.core.model.document.BaseUnit
import amf.core.parser.UnspecifiedReference
import amf.core.remote.{Cache, Context}
import amf.core.services.RuntimeCompiler
import amf.core.unsafe.PlatformSecrets
import amf.plugins.document.vocabularies.AMLPlugin
import amf.plugins.document.webapi.{Oas20Plugin, Raml10Plugin}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait AmfUtils extends  PlatformSecrets {

  def loadDialect(path: String): Future[BaseUnit] = {
    for {
      _      <- {
        AMF.registerPlugin(AMLPlugin)
        AMF.registerPlugin(Raml10Plugin)
        AMF.registerPlugin(Oas20Plugin)
        AMF.init()
      }
      parsed <- RuntimeCompiler(path,
        Some("application/yaml"),
        Some("AML 1.0"),
        Context(platform),
        UnspecifiedReference,
        Cache())
    } yield {
     parsed
    }
  }
}
