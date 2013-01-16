package de.jmaschad.storagesim.model.microcloud

import de.jmaschad.storagesim.Units
import de.jmaschad.storagesim.model.storage.StorageObject

object Job {
    var uploadJobCount = 0
    var downloadJobCount = 0
}

class Job(val storageObject: StorageObject, onFinish: Boolean => Unit) {
    var netUpSize: Option[Double] = None;
    var netDownSize: Option[Double] = None;
    var ioLoadSize: Option[Double] = None;
    var ioStoreSize: Option[Double] = None;

    private var success = true;

    def progressNetUp(progress: Double) = {
        netUpSize = netUpSize.map(_ - progress)
    }

    def progressNetDown(progress: Double) = {
        netDownSize = netDownSize.map(_ - progress)
    }

    def progressIoLoad(progress: Double) = {
        ioLoadSize = ioLoadSize.map(_ - progress)
    }

    def progressIoStore(progress: Double) = {
        ioStoreSize = ioStoreSize.map(_ - progress)
    }

    def setFailed() = { success = false }
    def hasFailed = !success
    def finish() = onFinish(success)
    def isDone = hasFailed ||
        List(netUpSize, netDownSize, ioLoadSize, ioStoreSize).map(_.forall(_ < 1 * Units.Byte)).forall(_ == true)

    override def toString = "%s for %s [%f]".format(getClass().getSimpleName(), storageObject, netUpSize.getOrElse(0.0).max(ioLoadSize.getOrElse(0.0)))
}

class UploadJob(storageObject: StorageObject, onFinish: Boolean => Unit) extends Job(storageObject, onFinish) {
    netUpSize = Some(storageObject.size)
    ioLoadSize = Some(storageObject.size)

    Job.uploadJobCount += 1

    override def finish() = {
        super.finish()
        Job.uploadJobCount -= 1
    }
}

class DownloadJob(storageObject: StorageObject, onFinish: Boolean => Unit) extends Job(storageObject, onFinish) {
    netDownSize = Some(storageObject.size)
    ioStoreSize = Some(storageObject.size)

    Job.downloadJobCount += 1

    override def finish() = {
        super.finish()
        Job.downloadJobCount -= 1
    }
}
