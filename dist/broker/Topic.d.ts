import { Partition } from "./Partition.js";
export declare class Topic {
    readonly name: string;
    private partitions;
    private roundRobinCounter;
    constructor(name: string, partitionCount: number);
    getPartition(partitionId: number): Partition;
    selectPartiton(key?: string): Partition;
    private hash;
    getPartitions(): Partition[];
}
//# sourceMappingURL=Topic.d.ts.map