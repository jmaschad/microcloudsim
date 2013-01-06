package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEntity
import de.jmaschad.storagesim.LoggingEntity
import org.cloudbus.cloudsim.core.SimEvent
import scala.collection.mutable
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import de.jmaschad.storagesim.model.distribution.RequestDistributor
import de.jmaschad.storagesim.model.microcloud.Status

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
  private val statusTracker = new StatusTracker
  private var eventHandler = onlineEventHandler _

  override def startEntity(): Unit = {
    log("started")
    send(getId(), Disposer.CheckStatusInterval, Disposer.CheckStatus)
  }

  override def shutdownEntity(): Unit = log("shutdown")

  override def processEvent(event: SimEvent): Unit = eventHandler(event)

  private def onlineEventHandler(event: SimEvent): Unit = event.getTag() match {
    case Disposer.Shutdown =>
      statusTracker.onlineClouds.keys.foreach(sendNow(_, MicroCloud.Shutdown))
      eventHandler = offlineEventHandler _

    case Disposer.MicroCloudStatus =>
      val status = (event.getData() match {
        case s: Status => s
        case _ => throw new IllegalArgumentException
      })
      statusTracker.online(event.getSource, status)
      distributor.statusUpdate(statusTracker.onlineClouds)

    case Disposer.MicroCloudShutdown =>
      statusTracker.offline(event.getSource)

    case Disposer.CheckStatus =>
      val goneOffline = statusTracker.check()
      send(getId(), Disposer.CheckStatusInterval, Disposer.CheckStatus)

    case Disposer.UserRequest =>
      val request = (event.getData() match {
        case r: UserObjectRequest => r
        case _ => throw new IllegalStateException
      })

      distributor.selectMicroCloud(request) match {
        case None =>
          sendNow(event.getSource(), User.RequestFailed, request)
        case Some(cloud) =>
          sendNow(cloud, MicroCloud.UserRequest, request)
      }

    case _ => log("dropped event " + event)
  }

  private def offlineEventHandler(event: SimEvent) = log("Offline! dropped event " + event)
}