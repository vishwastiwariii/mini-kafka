import { Partition } from "./Partition.js";
export class Topic {
    name;
    partitions = [];
    roundRobinCounter = 0;
    constructor(name, partitionCount) {
        this.name = name;
        for (let i = 0; i < partitionCount; i++) {
            this.partitions.push(new Partition(i));
        }
    }
    getPartition(partitionId) {
        const partition = this.partitions[partitionId];
        if (!partition) {
            throw new Error("No partition exists");
        }
        return partition;
    }
    selectPartiton(key) {
        let partitionId;
        if (key) {
            partitionId = this.hash(key) % this.partitions.length;
        }
        else {
            partitionId = this.roundRobinCounter % this.partitions.length;
            this.roundRobinCounter++;
        }
        return this.partitions[partitionId];
    }
    hash(key) {
        let hash = 0;
        for (let i = 0; i < key.length; i++) {
            hash = (hash * 31 + key.charCodeAt(i)) >>> 0;
        }
        return hash;
    }
    getPartitions() {
        return this.partitions;
    }
}
//# sourceMappingURL=Topic.js.map