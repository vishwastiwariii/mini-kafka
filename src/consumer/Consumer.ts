import { Broker } from "../broker/Broker.js";
import type { Message } from "../types/Message.js";

/**
 * Pull-based consumer bound to a single partition.
 * Delivery is at-least-once: poll() returns the message at the current
 * offset and keeps returning it until acknowledge() is called.
 */
export class Consumer {
    private currentOffset = 0;

    constructor (
        private broker: Broker,
        private topicName: string,
        private partitionId: number
    ) {
        // Fail fast if the topic or partition doesn't exist
        this.broker.getTopic(topicName).getPartition(partitionId)
    }

    async poll() : Promise<Message | null> {
        const topic = this.broker.getTopic(this.topicName)

        const partition = topic.getPartition(this.partitionId)

        return partition.read(this.currentOffset)
    }

    acknowledge(offset: number) {
        if(offset !== this.currentOffset) {
            throw new Error("Invalid Acknowledgement")
        }

        this.currentOffset++
    }

    /** Move the read position to an arbitrary offset (replay or skip). */
    seek(offset: number) {
        if(!Number.isInteger(offset) || offset < 0) {
            throw new Error("Invalid offset")
        }

        this.currentOffset = offset
    }

    getOffset() : number {
        return this.currentOffset
    }
}
