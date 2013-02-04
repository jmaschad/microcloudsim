package de.jmaschad.storagesim.model

import de.jmaschad.storagesim.model.processing.StorageDevice

class ResourceCharacteristics(bandwidth: Double, storageDevices: Seq[StorageDevice]) {
    def bandwidth(): Double = bandwidth
    def storageDevices(): Seq[StorageDevice] = storageDevices
}