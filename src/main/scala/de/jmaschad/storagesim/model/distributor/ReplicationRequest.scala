package de.jmaschad.storagesim.model.distributor

import java.util.Objects

object ReplicationRequest {
    def apply(source: Int, targets: Set[Int], bucket: String): ReplicationRequest = {
        new ReplicationRequest(source, targets, bucket)
    }
}

class ReplicationRequest(val source: Int, val targets: Set[Int], val bucket: String)