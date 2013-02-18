package de.jmaschad.storagesim.model.microcloud

import de.jmaschad.storagesim.model.distributor.Distributor
import de.jmaschad.storagesim.model.processing.StorageSystem
import de.jmaschad.storagesim.model.processing.Uploader
import de.jmaschad.storagesim.model.processing.Download
import de.jmaschad.storagesim.model.processing.Downloader
import de.jmaschad.storagesim.model.processing.ProcessingModel

class InterCloudHandler(
    log: String => Unit,
    send: (Int, Int, Object) => Unit,
    microCloud: MicroCloud,
    distributor: Distributor,
    storageSystem: StorageSystem,
    uploader: Uploader,
    downloader: Downloader,
    processing: ProcessingModel) {

    def processRequest(source: Int, data: Object) = {

    }
}