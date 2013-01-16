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

//object Log {
//    val formatter = new SimpleDateFormat
//    val logFile = Paths.get("logs", "log-" + Calendar.getInstance().getTimeInMillis()).toFile()
//    val fileWriter = new PrintWriter(logFile)
//
//    def line(identifier: String, line: String) = {
//        val out = "%.3f %s: %s%n".format(CloudSim.clock(), identifier, line)
//        print(out)
//        fileWriter.append(out)
//    }
//}

object Log {
    def line(identifier: String, line: String) = {
        print("%.3f %s: %s%n".format(CloudSim.clock(), identifier, line))
    }
}