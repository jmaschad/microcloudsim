package de.jmaschad.storagesim.model

import scala.collection.mutable
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import org.apache.commons.math3.distribution.UniformRealDistribution
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.LoggingEntity
import org.cloudbus.cloudsim.core.CloudSim

object User {
  private val Base = 10300
  val RequestFailed = Base + 1
  val RequestDone = Base + 2
}

class User(name: String, disposer: Disposer) extends SimEntity(name) with LoggingEntity {
  private val behaviors = mutable.ArrayBuffer.empty[Behavior]

  def addBehavior(behavior: Behavior) = behaviors += behavior

  override def startEntity(): Unit = {
    behaviors.foreach(b =>
      b.requests.foreach(req => send(disposer.getId(), req._1, Disposer.UserRequest, req._2)))
  }

  override def shutdownEntity(): Unit = {}

  override def processEvent(event: SimEvent): Unit = event.getTag() match {
    case User.RequestDone => done(event)
    case User.RequestFailed => failed(event)
    case _ => log("dropped event" + event)
  }

  private def done(event: SimEvent) = {
    event.getData() match {
      case GetObject(obj, user, time) => log("DONE GET %s in %.3fs".format(obj, CloudSim.clock - time))
      case PutObject(obj, user, time) => log("DONE PUT %s in %.3fs".format(obj, CloudSim.clock - time))
      case _ => throw new IllegalArgumentException
    }
  }

  private def failed(event: SimEvent) = {
    event.getData() match {
      case GetObject(obj, user, time) => log("FAILED GET %s in %.3fs".format(obj, CloudSim.clock - time))
      case PutObject(obj, user, time) => log("FAILED PUT %s in %.3fs".format(obj, CloudSim.clock - time))
      case _ => throw new IllegalArgumentException
    }
  }
}