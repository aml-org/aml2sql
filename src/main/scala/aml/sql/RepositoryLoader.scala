package aml.sql

import java.io.{BufferedWriter, File, FileWriter}
import java.nio.file.{Files, Path, Paths}

import amf.plugins.document.vocabularies.model.document.Dialect
import aml.sql.utils.AmfUtils

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class RepositoryLoader() extends AmfUtils {
  val globalSchemasDDL: mutable.ListBuffer[String] = mutable.ListBuffer()
  val globalConstraintsDDL: mutable.ListBuffer[String] = mutable.ListBuffer()
  val globalJoinDefinitionsDDL: mutable.ListBuffer[String] = mutable.ListBuffer()
  val globalDefinitionsDDL: mutable.ListBuffer[String] = mutable.ListBuffer()

  val globalR2RML: mutable.ListBuffer[String] = mutable.ListBuffer()

  def fromDirectory(path: String) = {
    var files = Files.walk(Paths.get(path)).iterator().asScala
    val schemaFiles = files.filter(f => f.endsWith("schema.yaml"))
    files = Files.walk(Paths.get(path)).iterator().asScala
    val futures = schemaFiles.map { f =>
      processSchemaFile(f).map { _ =>
        println(s"*** Processed ${f}")
      } recover {
        case e: Exception =>
          println(s"failed!!! ${e.getMessage}")
      }
    }

    Future.sequence(futures)
  }

  def wrapGlobal(str: String): String = {
    s"""
      |{
      |  "@context": {
      |    "rr": "http://www.w3.org/ns/r2rml#"
      |  },
      |  "@graph": [
      |    ${str}
      |  ]
      |}
      |""".stripMargin
  }

  def writeGlobalFiles(path: String): Unit = {
    // global SQL
    val globalSQL = /* globalSchemasDDL.distinct ++ */globalDefinitionsDDL.filter(_ != "").distinct ++ globalConstraintsDDL.distinct ++ globalJoinDefinitionsDDL.distinct
    val fSQL = new File(path).getAbsolutePath + File.separator + "schema.sql"
    println(s"*** GENERATING GLOBAL SQL AT ${fSQL}")
    writeFile(fSQL, globalSQL.mkString("\n\n"))

    // global r2rml
    val fR2RML = new File(path).getAbsolutePath + File.separator + "schema.r2rml"
    println(s"*** GENERATING GLOBAL R2RML AT ${fR2RML}")
    writeFile(fR2RML, wrapGlobal(globalR2RML.mkString(",\n")))
  }


  protected def processSchemaFile(f: Path) = {
    println(s"*** Loading schema file ${f.toFile.getAbsolutePath}")

    loadDialect("file://" + f.toFile.getAbsolutePath) map { case dialect: Dialect =>
      val database = new DataBaseParser(dialect).parse()
      val generatedSQL = SQLDDLGenerator.generate(database)
      globalSchemasDDL += SQLDDLGenerator.generateDeclarationSchemas(database)
      globalConstraintsDDL += SQLDDLGenerator.generateTableConstraints(database)
      globalJoinDefinitionsDDL += SQLDDLGenerator.generateDefinitionJoinTables(database)
      globalDefinitionsDDL += SQLDDLGenerator.generateDefinitionTables(database)

      val generatedR2RML = new R2RMLGenerator(database = database).generate()
      globalR2RML += new R2RMLGenerator(database = database).generate(global = true)

      var targetPath = f.toFile.getAbsolutePath.replace("schema.yaml", "database.sql")
      writeFile(targetPath, generatedSQL)
      targetPath = f.toFile.getAbsolutePath.replace("schema.yaml", "database.r2rml")
      writeFile(targetPath, generatedR2RML)
    }
  }

  protected def writeFile(filename: String, text: String) = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }
}

object RepositoryLoader {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Path to a directory containing CIM files must be provided as an argument")
      System.exit(1)
    }
    val path = args(0)
    println(s"\n\nProcessing directory $path\n\n")
    val loader = new RepositoryLoader()
    val result = loader.fromDirectory(path)
    Await.result(result, Duration.Inf)
    loader.writeGlobalFiles(path)
  }

}
