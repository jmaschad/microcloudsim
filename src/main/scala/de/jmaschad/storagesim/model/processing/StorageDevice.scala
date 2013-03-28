package de.jmaschad.storagesim.model.processing

class StorageDevice(val capacity: Double) {
    override def toString = "StorageDevice [%.0fMB free]".format(capacity)
}