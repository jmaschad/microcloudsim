package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import de.jmaschad.storagesim.LoggingEntity
import de.jmaschad.storagesim.model.microcloud.MicroCloudStatus
import org.cloudbus.cloudsim.core.SimEvent
import scala.collection.mutable
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.distribution.RequestDistributor

object Disposer {
  val StatusInterval = 1
  val CheckStatusInterval = 2 * StatusInterval

  val Base = 10100
  val Shutdown = Base + 1
  val MicroCloudStatus = Shutdown + 1
  val MicroCloudShutdown = MicroCloudStatus + 1
  val CheckStatus = MicroCloudShutdown + 1
  val UserRequest = CheckStatus + 1
}

class Disposer(name: String, distributor: RequestDistributor) extends SimEntity(name) with LoggingEntity {
  val onlineMicroClouds = mutable.Map.empty[Int, MicroCloudStatus]
  val statusTracker = new StatusTracker

  var online = true

  override def startEntity(): Unit = {
    log("started")
    send(getId(), Disposer.CheckStatusInterval, Disposer.CheckStatus)
  }

  override def shutdownEntity(): Unit = log("shutdown")

  override def processEvent(event: SimEvent): Unit = event.getTag() match {
    case Disposer.Shutdown =>
      onlineMicroClouds.keys.foreach(sendNow(_, MicroCloud.Shutdown))
      online = false

    case Disposer.MicroCloudStatus =>
      log("MicroCloud status update " + event)
      val status = (event.getData() match {
        case s: MicroCloudStatus => s
        case _ => throw new IllegalArgumentException
      })
      onlineMicroClouds += event.getSource -> status
      statusTracker online event.getSource

    case Disposer.MicroCloudShutdown =>
      log("MicroCloud will shutdown")
      onlineMicroClouds -= event.getSource
      statusTracker offline event.getSource

    case Disposer.CheckStatus =>
      log("checking MicroCloudStatus")
      val goneOffline = statusTracker.check()
      onlineMicroClouds --= goneOffline
      if (online)
        send(getId(), Disposer.CheckStatusInterval, Disposer.CheckStatus)

    case Disposer.UserRequest =>
      //      log("forewarding user request")
      val request = (event.getData() match {
        case r: UserObjectRequest => r
        case _ => throw new IllegalStateException
      })
      val microCloud = distributor.selectMicroCloud(request, onlineMicroClouds)
      sendNow(microCloud, MicroCloud.UserRequest, request)

    case _ => log("dropped event " + event)
  }
}