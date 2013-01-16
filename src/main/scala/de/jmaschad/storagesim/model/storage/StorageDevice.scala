package de.jmaschad.storagesim.model.storage

class StorageDevice(bandwidth: Double, capacity: Double) {
    private var allocated = 0.0
    private var accessorCount = 0

    def allocate(size: Double) = {
        allocated += size
    }

    def deallocate(size: Double) = {
        allocated -= size
    }

    def hasAvailableSpace(size: Double): Boolean = (capacity - allocated) > size

    def loadThroughput: Double = bandwidth / accessorCount

    def storeThroughput: Double = loadThroughput

    def addAccessor() = accessorCount += 1

    def removeAccessor() = accessorCount -= 1

    override def toString = "StorageDevice [%.0fMB free]".format(capacity - allocated)
}