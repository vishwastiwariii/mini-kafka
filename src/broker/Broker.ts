import type { Message } from "../types/Message.js";
import { Topic } from "./Topic.js";
import { v4 as uuid} from 'uuid'


export class Broker {
    private topics: Map<string, Topic> = new Map()

    createTopic(
        name: string, 
        partitionCount: number, 
    ) {
        if(this.topics.has(name)) {
            throw new Error("Topic already exists")
        }

        const topic = new Topic(name, partitionCount)

        this.topics.set(name, topic)

        return topic
    }

    getTopics(name: string) : Topic {
        const topic = this.topics.get(name)

        if(!topic) {
            throw new Error("Topics not found")
        }

        return topic
    }

    publish(
        topicName: string, 
        key: string | undefined, 
        value: any
    ) : Message {
        const topic = this.getTopics(topicName)

        const partition = topic.selectPartiton(key)

        const message = partition.append({
            id: uuid(), 
            topic: topicName, 
            partition: partition.partitionId, 
            key, 
            value, 
            timestamp: Date.now(),
        })

        return message
    }
}