package de.jmaschad.storagesim.model.microcloud

import org.apache.commons.math3.distribution.RealDistribution

class MicroCloudFailureBehavior(
    cloudFailureDistribution: RealDistribution,
    cloudRepairTimeDistribution: RealDistribution,
    diskFailureDistribution: RealDistribution,
    diskRepairTimeDistribution: RealDistribution) {

    def timeToCloudFailure: Double = cloudFailureDistribution.sample().max(0.0)

    def timeToCloudRepair: Double = cloudRepairTimeDistribution.sample().max(0.0)

    def timeToDiskFailur: Double = diskFailureDistribution.sample().max(0.0)

    def timeToDiskRepair: Double = diskRepairTimeDistribution.sample().max(0.0)
}