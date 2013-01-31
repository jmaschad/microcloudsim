package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.storage.StorageObject
import de.jmaschad.storagesim.model.storage.StorageSystem
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.user.RequestType
import MicroCloud._
import de.jmaschad.storagesim.model.distributor.ReplicationRequest
import de.jmaschad.storagesim.model.distributor.Distributor

object MicroCloud {
    private val Base = 10200
    val ProcUpdate = Base + 1
    val Boot = ProcUpdate + 1
    val Shutdown = Boot + 1
    val Kill = Shutdown + 1
    val UserRequest = Kill + 1
    val MicroCloudStatus = UserRequest + 1
    val SendReplica = MicroCloudStatus + 1
    val StoreReplica = SendReplica + 1
}

class MicroCloud(
    name: String,
    resourceCharacteristics: MicroCloudResourceCharacteristics,
    initialObjects: Iterable[StorageObject],
    failureBehavior: MicroCloudFailureBehavior,
    disposer: Distributor) extends SimEntity(name) {

    private[microcloud] val log = Log.line("MicroCloud '%s'".format(getName), _: String)
    private val storageSystem = new StorageSystem(resourceCharacteristics.storageDevices, initialObjects)
    private val processing = new ResourceProvisioning(storageSystem, resourceCharacteristics.bandwidth, this)
    private var state: MicroCloudState = new OfflineState

    var lastChainUpdate: Option[SimEvent] = None

    def scheduleProcessingUpdate(delay: Double) = {
        lastChainUpdate.map(CloudSim.cancelEvent(_))
        lastChainUpdate = Some(schedule(getId(), delay, ProcUpdate))
    }

    def status = Status(storageSystem.buckets)

    override def startEntity(): Unit = {
        send(getId(), 0.0, Boot)
    }

    override def shutdownEntity() = {
        log(processing.jobCount + " running jobs on shutdown")
    }

    override def processEvent(event: SimEvent): Unit = event.getTag match {
        case ProcUpdate =>
            log("chain update")
            processing.update()
        case _ => state.process(event)
    }

    override def toString = "%s %s".format(getClass.getSimpleName, getName)

    private def switchState(newState: MicroCloudState): Unit = {
        state = newState
    }

    private trait MicroCloudState {
        def process(event: SimEvent): Unit

        protected def stateLog(message: String): Unit = log("[%s] %s".format(getClass().getSimpleName(), message))
    }

    private class OfflineState extends MicroCloudState {
        def process(event: SimEvent): Unit = event.getTag match {
            case MicroCloud.Boot =>
                stateLog("received boot request")
                sendNow(getId, MicroCloud.MicroCloudStatus)
                send(getId, failureBehavior.timeToCloudFailure, Kill)
                switchState(new OnlineState)

            case _ => stateLog("dropped event " + event)
        }
    }

    private class OnlineState extends MicroCloudState {
        private var dueReplicationAcks = scala.collection.mutable.Map.empty[String, Seq[StorageObject]]

        def process(event: SimEvent): Unit = event.getTag() match {
            case UserRequest =>
                val request = event.getData() match {
                    case r: Request => r
                    case _ => throw new IllegalStateException
                }
                stateLog("received %s".format(request))
                processUserRequest(request)

            case MicroCloudStatus =>
                sendNow(disposer.getId(), Distributor.MicroCloudStatus, status)
                send(getId(), Distributor.StatusInterval, MicroCloudStatus)

            case Shutdown =>
                stateLog("received shutdown request")
                sendNow(disposer.getId(), Distributor.MicroCloudShutdown)
                switchState(new OfflineState)

            case Kill =>
                stateLog("received kill request")
                processing.clear()
                send(getId, failureBehavior.timeToCloudRepair, Boot)
                switchState(new OfflineState)

            case SendReplica =>
                val replicationRequest = event.getData() match {
                    case req: ReplicationRequest => req
                    case _ => throw new IllegalStateException
                }
                stateLog("received request to send replicate of " +
                    replicationRequest.bucket + " to " + replicationRequest.targets.map(CloudSim.getEntityName(_)).mkString(","))
                val objects = storageSystem.bucket(replicationRequest.bucket)
                replicationRequest.targets.foreach(target => {
                    objects.foreach(obj => {
                        load(obj)
                    })
                    sendNow(target, MicroCloud.StoreReplica, (replicationRequest, objects))
                })

            case StoreReplica =>
                val (request, objects) = event.getData() match {
                    case (req: ReplicationRequest, objs: Seq[StorageObject]) => (req, objs)
                    case _ => throw new IllegalStateException
                }
                stateLog(CloudSim.getEntityName(event.getSource()) + " send request to store replica for " + objects.mkString(","))

                // store all objects and notify the disposer when the last is saved
                val objectsToStore = scala.collection.mutable.Set.empty ++ objects
                objects.foreach(obj => store(obj, success =>
                    if (success) {
                        assert(objectsToStore.contains(obj))
                        objectsToStore -= obj
                        if (objectsToStore.isEmpty) {
                            sendNow(disposer.getId(), Distributor.ReplicationFinished, request)
                        }
                    } else {
                        throw new IllegalStateException
                    }))

            case _ => log("dropped event " + event)
        }

        def processUserRequest(request: Request): Unit = {
            def onFinish(success: Boolean) =
                sendNow(request.user.getId, if (success) User.RequestDone else User.RequestFailed, request)

            request.requestType match {
                case RequestType.Get =>
                    load(request.storageObject, onFinish(_))

                case RequestType.Put =>
                    store(request.storageObject, onFinish(_))

                case _ => throw new IllegalArgumentException
            }
        }

        def load(obj: StorageObject, onFinish: (Boolean => Unit) = _ => {}) =
            storageSystem.loadTransaction(obj) match {
                case Some(transaction) =>
                    processing.add(new UploadJob(obj, success => {
                        transaction.complete()
                        onFinish(success)
                    }))

                case None => onFinish(false)
            }

        def store(storageObject: StorageObject, onFinish: (Boolean => Unit) = _ => {}) =
            storageSystem.storeTransaction(storageObject) match {
                case Some(trans) =>
                    processing.add(new DownloadJob(storageObject,
                        success => {
                            if (success) trans.complete() else trans.abort()
                            onFinish(success)
                        }))
                case None =>
                    onFinish(false)
            }
    }
}
