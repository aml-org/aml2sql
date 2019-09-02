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
  val globalSQL: mutable.ListBuffer[String] = mutable.ListBuffer()
  val globaR2RML: mutable.ListBuffer[String] = mutable.ListBuffer()

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

  def writeGlobalFiles(path: String): Unit = {
    val fSql = new File(path).getAbsolutePath + File.separator + "model.sql"
    println(s"*** GENERATING GLOBAL SQL AT ${fSql}")
    writeFile(fSql, globalSQL.mkString("\n\n"))
    val fR2rml = new File(path).getAbsolutePath + File.separator + "model.r2rml"
    println(s"*** GENERATING GLOBAL R2RML AT ${fR2rml}")
    writeFile(fR2rml, globaR2RML.mkString("\n\n"))
  }


  protected def processSchemaFile(f: Path) = {
    println(s"*** Loading schema file ${f.toFile.getAbsolutePath}")

    loadDialect("file://" + f.toFile.getAbsolutePath) map { case dialect: Dialect =>
      val database = new DataBaseParser(dialect).parse()
      val generatedSQL = SQLDDLGenerator.generate(database)
      globalSQL += generatedSQL
      val generatedR2RML = new R2RMLGenerator(database = database).generate()
      globaR2RML += generatedR2RML

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
