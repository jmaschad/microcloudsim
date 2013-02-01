package de.jmaschad.storagesim.model.transfer

import de.jmaschad.storagesim.Units

trait Workload {
    def process(timeSpan: Double): Workload
    def expectedCompletion: Double
    def isDone: Boolean
}

private[transfer] abstract class NetworkTransfer(size: Double, totalBandwidth: Double) extends Workload {
    override def expectedCompletion: Double = size / bandwidth
    override def isDone = size < 1 * Units.Byte
    override def process(timeSpan: Double) =
        Upload(size - progress(timeSpan), totalBandwidth)

    protected def activeTransfers: Int

    private def bandwidth = totalBandwidth / activeTransfers
    private def progress(timeSpan: Double) = timeSpan * bandwidth
}

object Upload {
    private var activeUploads = Set.empty[Upload];

    def apply(size: Double, totalBandwidth: Double) = new Upload(size, totalBandwidth)
}

private[transfer] class Upload(size: Double, totalBandwidth: Double)
    extends NetworkTransfer(size, totalBandwidth) {

    if (!isDone) Upload.activeUploads += this

    override protected def activeTransfers = Upload.activeUploads.size

    override def process(timeSpan: Double) = {
        Upload.activeUploads -= this
        super.process(timeSpan)
    }
}

object Download {
    private var activeDownloads = Set.empty[Download]

    def apply(size: Double, totalBandwidth: Double) = new Download(size, totalBandwidth)
}

private[transfer] class Download(size: Double, totalBandwidth: Double)
    extends NetworkTransfer(size, totalBandwidth) {

    if (!isDone) Download.activeDownloads += this

    override def process(timeSpan: Double) = {
        Download.activeDownloads -= this
        super.process(timeSpan)
    }

    override protected def activeTransfers = Download.activeDownloads.size
}

object DiskIO {
    def apply(size: Double, throughput: => Double) = new DiskIO(size, throughput)
}

private[transfer] class DiskIO(size: Double, throughput: => Double) extends Workload {
    override def process(timeSpan: Double) = DiskIO(size - progress(timeSpan), throughput)
    override def expectedCompletion: Double = size / throughput
    override def isDone = size < 1 * Units.Byte

    private def progress(timeSpan: Double) = timeSpan * throughput
}