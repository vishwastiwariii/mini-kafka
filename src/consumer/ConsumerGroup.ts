import type { Broker } from "../broker/Broker.js";
import { Consumer } from "./Consumer.js";


export class ConsumerGroup {

    private consumers: Map<string, Consumer> = new Map()
    private assignments: Map<string, number[]> = new Map()
    private commitedOffsets: Map<number, number> = new Map()

    private generation = 0 //changes when ownership of partition changes

    private partitionIds: number[] = []

    private closed = false

    constructor (
        private readonly broker: Broker,
        private readonly groupId: string, 
        private readonly topicName: string
    ) {
        if(typeof topicName != "string" ||
            topicName.trim().length === 0
        ) {
            throw new Error(`Invalid Topic Name: ${topicName}`)
        }

        if(typeof groupId != "string" ||
            groupId.trim().length === 0
        ) {
            throw new Error(`Invalid groupId: ${groupId}`)
        }

        const topic = this.broker.getTopic(topicName)

        for(const partition of topic.getPartitions()) {
            this.partitionIds.push(partition.partitionId)

            this.commitedOffsets.set(
                partition.partitionId,
                0
            )
        }
    }

    registerConsumer(consumerId: string) : Consumer {
        this.assertOpen()

        if(
            typeof consumerId != "string" ||
            consumerId.trim().length === 0
        ) {
            throw new Error(`Invalid Consumer Id: ${consumerId}`)
        }

        if(this.consumers.has(consumerId)){
            throw new Error(`Consumer already Exists`)
        }

        const consumer = new Consumer(this.broker)

        this.consumers.set(consumerId, consumer)

        this.rebalance()

        return consumer
    }

    removeConsumer(consumerId: string) : boolean {
        if(
            typeof consumerId != "string" ||
            consumerId.trim().length === 0
        ) {
            throw new Error(`Invalid Consumer Id: ${consumerId}`)
        }

        const consumer = this.consumers.get(consumerId)

        if(!consumer) {
            return false
        }

        this.generation++

        this.revoke(consumer)

        this.consumers.delete(consumerId)

        this.assignments.delete(consumerId)

        this.rebalance()

        return true
    }

    // Range assignment: consumers sorted by id each get a contiguous
    // chunk of partitions (earlier consumers take the remainder).
    // Every rebalance bumps the generation, which invalidates the commit
    // callbacks handed out earlier, so a stale consumer can't commit.
    // Consumers restart from the group's committed offsets, so messages
    // fetched but not acknowledged are redelivered.
    rebalance() {
        this.generation++

        this.assignments.clear()

        const consumerIds = [...this.consumers.keys()].sort()

        if(consumerIds.length === 0) {
            return
        }

        const total = this.partitionIds.length
        const base = Math.floor(total / consumerIds.length)
        const extra = total % consumerIds.length

        let next = 0

        consumerIds.forEach((consumerId, index) => {
            const count = base + (index < extra ? 1 : 0)

            this.assignments.set(
                consumerId,
                this.partitionIds.slice(next, next + count)
            )

            next += count
        })

        const generation = this.generation

        for(const [consumerId, consumer] of this.consumers) {
            const assigned = this.assignments.get(consumerId) ?? []

            const initialCommittedOffsets: Record<number, number> = {}

            for(const partitionId of assigned) {
                initialCommittedOffsets[partitionId] =
                    this.commitedOffsets.get(partitionId) ?? 0
            }

            consumer.assign({
                topicName: this.topicName,
                assignedPartitionIds: [...assigned],
                initialCommittedOffsets,
                commitOffset: (partitionId, nextOffset) =>
                    this.commitOffset(
                        consumerId,
                        generation,
                        partitionId,
                        nextOffset
                    )
            })
        }
    }

    // Called (through the callback given to each consumer) when a consumer
    // acknowledges a message. Throws if the caller no longer owns the
    // partition, so Consumer leaves its local state untouched.
    commitOffset(
        consumerId: string,
        generation: number,
        partitionId: number,
        nextOffset: number
    ) {
        this.assertOpen()

        if(!this.consumers.has(consumerId)) {
            throw new Error(`Consumer ${consumerId} is not part of the group`)
        }

        if(generation !== this.generation) {
            throw new Error(`Stale generation: consumer ${consumerId} must rejoin`)
        }

        if(!this.assignments.get(consumerId)?.includes(partitionId)) {
            throw new Error(
                `Partition ${partitionId} is not assigned to consumer ${consumerId}`
            )
        }

        const committed = this.commitedOffsets.get(partitionId)

        if(committed === undefined) {
            throw new Error(`Unknown partition: ${partitionId}`)
        }

        if(!Number.isInteger(nextOffset) || nextOffset < 0) {
            throw new Error(`Invalid offset: ${nextOffset}`)
        }

        if(nextOffset < committed) {
            throw new Error(
                `Cannot move committed offset back from ${committed} to ${nextOffset}`
            )
        }

        const maximum = this.broker
            .getTopic(this.topicName)
            .getPartition(partitionId)
            .getLatestOffset() + 1

        if(nextOffset > maximum) {
            throw new Error(
                `Offset ${nextOffset} exceeds available messages in partition ${partitionId}`
            )
        }

        this.commitedOffsets.set(partitionId, nextOffset)
    }

    getConsumer(consumerId: string) : Consumer {
        const consumer = this.consumers.get(consumerId)

        if(!consumer) {
            throw new Error(`Consumer doesn't exists`)
        }

        return consumer
    }

    // Copy of consumerId -> partition ids
    getAssignments() : Map<string, number[]> {
        const copy = new Map<string, number[]>()

        for(const [consumerId, partitionIds] of this.assignments) {
            copy.set(consumerId, [...partitionIds])
        }

        return copy
    }

    // Copy of partitionId -> next offset to consume
    getCommitedOffsets() : Map<number, number> {
        return new Map(this.commitedOffsets)
    }

    getGeneration() : number {
        return this.generation
    }

    // Revoke every consumer and stop accepting members or commits.
    close() {
        if(this.closed) {
            return
        }

        this.closed = true

        this.generation++

        for(const consumer of this.consumers.values()) {
            this.revoke(consumer)
        }

        this.consumers.clear()

        this.assignments.clear()
    }

    private revoke(consumer: Consumer) {
        consumer.assign({
            topicName: this.topicName,
            assignedPartitionIds: [],
            initialCommittedOffsets: {},
            commitOffset: () => {
                throw new Error(`Consumer assignment is revoked`)
            }
        })
    }

    private assertOpen() {
        if(this.closed) {
            throw new Error(`Consumer group ${this.groupId} is closed`)
        }
    }
}
