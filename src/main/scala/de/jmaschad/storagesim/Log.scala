package de.jmaschad.storagesim

import org.cloudbus.cloudsim.core.CloudSim
import java.io.File
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Calendar
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Date
import scala.util.Random
import java.io.PrintWriter
import java.nio.charset.Charset
import java.nio.file.Path

object Log {
    private var fileWriter: Option[PrintWriter] = None

    def open(logFile: Path): Unit =
        fileWriter = Some(new PrintWriter(Files.newBufferedWriter(logFile, Charset.forName("UTF-8"))))

    def close(): Unit =
        fileWriter.foreach(_.close)

    def line(identifier: String, line: String) = fileWriter match {
        case Some(writer) =>
            val out = "%.3f %s: %s".format(CloudSim.clock(), identifier, line)
            println(out)
            writer.println(out)
        case None =>
            throw new IllegalStateException
    }
}