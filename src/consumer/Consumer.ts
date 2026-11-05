import { Broker } from "../broker/Broker.js";
import type { Message } from "../types/Message.js";

export class Consumer {
    private currentOffset = 0;

    constructor (
        private broker: Broker,
        private topicName: string,
        private partitionId: number
    ) {}

    async poll() : Promise<Message | null> {
        const topic = this.broker.getTopics(this.topicName)

        const partition = topic.getPartition(this.partitionId)

        const message = partition.read(this.currentOffset)

        return message
    }

    acknowledge(offset: number) {
        if(offset !== this.currentOffset) {
            throw new Error("Invalid Acknowledgement")
        }

        this.currentOffset++
    }
}