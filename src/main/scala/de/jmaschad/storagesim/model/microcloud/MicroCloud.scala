package de.jmaschad.storagesim.model.microcloud

import org.cloudbus.cloudsim.core.CloudSim
import org.cloudbus.cloudsim.core.SimEntity
import org.cloudbus.cloudsim.core.SimEvent
import de.jmaschad.storagesim.Log
import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.distributor.ReplicationRequest
import de.jmaschad.storagesim.model.user.Request
import de.jmaschad.storagesim.model.user.RequestType
import de.jmaschad.storagesim.model.user.User
import de.jmaschad.storagesim.model.processing.Workload
import de.jmaschad.storagesim.model.processing.StorageObject
import de.jmaschad.storagesim.model.processing.ProcessingModel
import de.jmaschad.storagesim.model.processing.StorageSystem
import MicroCloud._
import de.jmaschad.storagesim.model.processing.Upload
import de.jmaschad.storagesim.model.processing.DiskIO
import de.jmaschad.storagesim.model.processing.Download
import de.jmaschad.storagesim.model.processing.Job
import org.cloudbus.cloudsim.core.predicates.PredicateType
import de.jmaschad.storagesim.model.processing.TransferModel

object MicroCloud {
    private val Base = 10200
    val ProcUpdate = Base + 1
    val Boot = ProcUpdate + 1
    val Shutdown = Boot + 1
    val Kill = Shutdown + 1
    val MicroCloudStatus = Kill + 1
    val UserRequest = MicroCloudStatus + 1
    val SendReplica = UserRequest + 1
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
    private val processing = new ProcessingModel(log, scheduleProcessingUpdate(_), resourceCharacteristics.bandwidth)
    private val transferModel = new TransferModel((target, tag, data) => sendNow(target, tag, data), this, processing)
    private var state: MicroCloudState = new OfflineState

    def scheduleProcessingUpdate(delay: Double) = {
        CloudSim.cancelAll(getId(), new PredicateType(ProcUpdate))
        schedule(getId(), delay, ProcUpdate)
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
                send(getId, TransferModel.TickDelay, TransferModel.Tick)
                switchState(new OnlineState)

            case _ => stateLog("dropped event " + event)
        }
    }

    private class OnlineState extends MicroCloudState {
        private var dueReplicationAcks = scala.collection.mutable.Map.empty[String, Seq[StorageObject]]

        def process(event: SimEvent): Unit = event.getTag() match {
            case TransferModel.Tick =>
                transferModel.tick()

            case TransferModel.Transfer =>
                transferModel.process(event.getSource(), event.getData())

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
                storageSystem.reset()
                send(getId, failureBehavior.timeToCloudRepair, Boot)
                switchState(new OfflineState)

            case UserRequest =>
                val request = event.getData() match {
                    case r: Request => r
                    case _ => throw new IllegalStateException
                }
                stateLog("received %s".format(request))
                processUserRequest(request)

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
                        load(obj, replicationRequest.source)
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
            def onFinish(success: Boolean) = success match {
                case true =>
                    log(request + " done")
                    sendNow(request.user.getId, User.RequestDone, request)
                case false =>
                    log(request + " failed")
                    sendNow(request.user.getId, User.RequestFailed, request)
            }

            request.requestType match {
                case RequestType.Get =>
                    load(request.storageObject, request.user.getId, onFinish(_))

                case RequestType.Put =>
                    store(request.storageObject, onFinish(_))

                case _ => throw new IllegalArgumentException
            }
        }

        def load(obj: StorageObject, target: Int, onFinish: (Boolean => Unit) = _ => {}) =
            storageSystem.loadTransaction(obj) match {
                case Some(trans) =>
                    transferModel.startUpload(trans, target)
                case None => onFinish(false)
            }

        def store(obj: StorageObject, onFinish: (Boolean => Unit) = _ => {}) =
            storageSystem.storeTransaction(obj) match {
                case Some(trans) =>
                    transferModel.expectDownload(trans)
                case None => onFinish(false)
            }
    }
}
