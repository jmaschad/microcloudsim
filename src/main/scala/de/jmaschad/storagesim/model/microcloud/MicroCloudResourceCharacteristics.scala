package de.jmaschad.storagesim.model.microcloud

import de.jmaschad.storagesim.model.transfer.StorageDevice

class MicroCloudResourceCharacteristics(bandwidth: Double, storageDevices: Seq[StorageDevice]) {
    def bandwidth(): Double = bandwidth
    def storageDevices(): Seq[StorageDevice] = storageDevices
}