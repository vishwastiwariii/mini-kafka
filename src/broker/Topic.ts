import { Partition } from "./Partition.js";


export class Topic {
    private partitions: Partition[] = []

    private roundRobinCounter = 0; 

    constructor (
        public readonly name: string, 
        partitionCount: number
    ) {

        if(name.length == 0 || name.trim().length == 0) {
            throw new Error("Name can't be empty string")
        }

        if(partitionCount < 0 || Number.isNaN(partitionCount) 
            || partitionCount % 1 != 0) {
            throw new Error("Invalid Partition Count")
        }

        for(let i=0; i < partitionCount; i++) {
            this.partitions.push(
                new Partition(i)
            )
        }
    }

    getPartition(partitionId: number) : Partition {
        const partition =  this.partitions[partitionId]

        if(!partition) {
            throw new Error("No partition exists")
        }

        return partition
    }

    selectPartition(key?: string) : Partition {
        let partitionId: number 

        if (key || key == "") {
            partitionId = this.hash(key) % this.partitions.length
        } else {
            partitionId = this.roundRobinCounter % this.partitions.length

            this.roundRobinCounter++
        }

        return this.partitions[partitionId]!
    }

    private hash(key: string) : number {
        let hash = 0; 

        for(let i=0; i < key.length ; i++) {
            hash = (hash * 31 + key.charCodeAt(i)) >>> 0; 
        }

        return hash
    }

    getPartitions() : Partition[] {
        const partitions = [...this.partitions] 

        return partitions
    }
}