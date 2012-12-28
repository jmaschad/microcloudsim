package de.jmaschad.storagesim.model

import de.jmaschad.storagesim.model.microcloud.MicroCloud

class StatusTracker {
  val MaxChecks = 2
  var missingUpdates = Map.empty[Int, Int]

  def online(microCloud: Int): Unit = {
    val missing = missingUpdates.getOrElse(microCloud, 0) - 1
    missingUpdates = (missingUpdates - microCloud) + (microCloud -> missing.max(0))
  }

  def offline(microCloud: Int): Unit = {
    missingUpdates -= microCloud
  }

  def check(): Seq[Int] = {
    missingUpdates = missingUpdates.mapValues(_ + 1)

    val offline = missingUpdates.filter(_._2 >= MaxChecks)
    missingUpdates --= offline.keys

    offline.keys.toSeq
  }
}