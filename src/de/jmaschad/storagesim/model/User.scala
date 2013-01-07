package de.jmaschad.storagesim.model

import scala.collection.mutable
import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.behavior.Behavior
import de.jmaschad.storagesim.model.request.GetRequest
import de.jmaschad.storagesim.model.request.PutRequest
import de.jmaschad.storagesim.model.request.Request
import org.cloudbus.cloudsim.core.SimEntity
import de.jmaschad.storagesim.Log

object User {
  private val Base = 10300
  val RequestFailed = Base + 1
  val RequestDone = Base + 2
}

class User(name: String, disposer: Disposer) extends SimEntity(name) {
  private val behaviors = mutable.ArrayBuffer.empty[Behavior]
  private val log = Log.line("User '%s'".format(getName), _: String)

  def addBehavior(behavior: Behavior) = behaviors += behavior

  override def startEntity(): Unit = {
    behaviors.foreach(b =>
      b.requests.foreach(req => send(disposer.getId(), req._1, Disposer.UserRequest, req._2)))
  }

  override def shutdownEntity() = {}

  override def processEvent(event: SimEvent): Unit = event.getTag() match {
    case User.RequestDone => done(event)
    case User.RequestFailed => failed(event)
    case _ => log("dropped event" + event)
  }

  private def logReq(req: Request, success: Boolean) = {
    log("%s %s in %.3fs".format(if (success) "SUCCSESS" else "FAILED", req, CloudSim.clock() - req.time))
  }

  private def done(event: SimEvent) = {
    event.getData() match {
      case req: GetRequest => logReq(req, true)
      case req: PutRequest => logReq(req, true)
      case _ => throw new IllegalArgumentException
    }
  }

  private def failed(event: SimEvent) = {
    event.getData() match {
      case req: GetRequest => logReq(req, false)
      case req: PutRequest => logReq(req, false)
      case _ => throw new IllegalArgumentException
    }
  }
}