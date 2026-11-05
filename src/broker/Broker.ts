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

    getTopic(name: string) : Topic {
        const topic = this.topics.get(name)

        if(!topic) {
            throw new Error(`Topic not found: ${name}`)
        }

        return topic
    }

    listTopics() : string[] {
       const topics = [...this.topics.keys()]

       return topics
    }

    deleteTopic(name: string) : boolean {
        return this.topics.delete(name)
    }

    publish(
        topicName: string, 
        key: string | undefined, 
        value: any
    ) : Message {
        const topic = this.getTopic(topicName)

        const partition = topic.selectPartition(key)

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