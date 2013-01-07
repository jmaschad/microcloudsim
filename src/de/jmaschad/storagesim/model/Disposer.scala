package de.jmaschad.storagesim.model

import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.distribution.RequestDistributor
import de.jmaschad.storagesim.model.microcloud.Status
import de.jmaschad.storagesim.model.request.Request
import de.jmaschad.storagesim.model.microcloud.MicroCloud
import org.cloudbus.cloudsim.core.SimEntity
import de.jmaschad.storagesim.Log
import org.cloudbus.cloudsim.core.CloudSim

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

class Disposer(name: String, distributor: RequestDistributor) extends SimEntity(name) {
  private val statusTracker = new StatusTracker
  private var eventHandler = onlineEventHandler _

  private val log = Log.line("Disposer '%s'".format(getName), _: String)

  override def startEntity(): Unit = {
    send(getId(), Disposer.CheckStatusInterval, Disposer.CheckStatus)
  }

  override def shutdownEntity() = {}

  override def processEvent(event: SimEvent): Unit = eventHandler(event)

  private def onlineEventHandler(event: SimEvent): Unit = event.getTag() match {
    case Disposer.Shutdown =>
      log("shutdown now")
      statusTracker.onlineClouds.keys.foreach(sendNow(_, MicroCloud.Shutdown))
      eventHandler = offlineEventHandler _

    case Disposer.MicroCloudStatus =>
      val status = (event.getData() match {
        case s: Status => s
        case _ => throw new IllegalArgumentException
      })
      log("status update from %s: %s".format(sourceEntity(event), status))
      statusTracker.online(event.getSource, status)
      distributor.statusUpdate(statusTracker.onlineClouds)

    case Disposer.MicroCloudShutdown =>
      log("shutdown notification from %s".format(sourceEntity(event)))
      statusTracker.offline(event.getSource)

    case Disposer.CheckStatus =>
      send(getId(), Disposer.CheckStatusInterval, Disposer.CheckStatus)

    case Disposer.UserRequest =>
      val request = (event.getData() match {
        case r: Request => r
        case _ => throw new IllegalStateException
      })

      distributor.selectMicroCloud(request) match {
        case None =>
          log("no cloud found for %s".format(request))
          sendNow(event.getSource(), User.RequestFailed, request)
        case Some(cloud) =>
          log("foreward %s to %s".format(request, CloudSim.getEntity(cloud)))
          sendNow(cloud, MicroCloud.UserRequest, request)
      }

    case _ => log("dropped event " + event)
  }

  private def sourceEntity(event: SimEvent) = CloudSim.getEntity(event.getSource())

  private def offlineEventHandler(event: SimEvent) = log("Offline! dropped event " + event)
}