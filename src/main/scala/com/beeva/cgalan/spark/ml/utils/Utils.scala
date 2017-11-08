package com.beeva.cgalan.spark.ml.utils

import java.io._
import java.net.URI
import java.nio.file.{Files, Paths}

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by cristiangalan on 4/03/17.
  */
object Utils {

  private val LOGGER = Logger(LoggerFactory.getLogger(this.getClass))

  /** getTime
    *
    * @param function The method to execute
    * @tparam F The return of function
    * @return Tuple with time in seconds and the return of function
    */
  def getTime[F](function: => F): (Long, F) = {
    val start = System.nanoTime()
    val result = function
    val end = System.nanoTime()
    (end - start, result)
  }

  /**
    * existPath.
    *
    * @param paths The paths that check
    * @return true if it exists
    *
    **/
  def existPath(paths: String*): Boolean = {
    var tmpPath = ""
    for (path <- paths) {
      tmpPath = Paths.get(tmpPath, path).toString
    }
    Files.exists(Paths.get(tmpPath))
  }

  /**
    * listDirectories.
    *
    * @param dir  The parent file
    * @param name The name file
    * @return A list of files or None
    *
    **/
  def listDirectories(dir: String, name: String): Option[List[File]] = {
    return listDirectories(Paths.get(dir, name).toString)
  }

  /**
    * listDirectories.
    *
    * @param path The path
    * @return A list of files or None
    **/
  def listDirectories(path: String): Option[List[File]] = {
    val file = new File(path)
    if (file.exists && file.isDirectory) {
      Some(file.listFiles.filter(_.isDirectory).toList)
    } else {
      None
    }
  }

  /**
    * getResourceAsURI.
    *
    * @param path The path
    * @return An uri
    **/
  def getResourceAsURI(path: String): URI = getClass.getResource("/" + path).toURI

  /**
    * getResourceAsFile.
    *
    * @param path The path
    * @return An uri
    **/
  def getResourceAsFile(path: String): File = {
    var in = None: Option[InputStream]
    var out = None: Option[FileOutputStream]

    import java.io.File
    val temp = File.createTempFile("temp", ".tmp")

    try {
      in = Some(getClass.getResourceAsStream("/" + path))
      out = Some(new FileOutputStream(temp))
      var c = 0
      while ( {
        c = in.get.read
        c != -1
      }) {
        out.get.write(c)
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (in.isDefined) in.get.close()
      if (out.isDefined) out.get.close()
    }

    temp
  }
}