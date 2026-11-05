import { ConsumerGroup } from "../consumer/ConsumerGroup.js";
import type { Message } from "../types/Message.js";
import { Topic } from "./Topic.js";
import { v4 as uuid} from 'uuid'


export class Broker {
    private topics: Map<string, Topic> = new Map()

    private consumerGroups: Map<string, ConsumerGroup> = new Map()

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
        // Groups bound to a deleted topic can no longer commit or poll
        for(const [groupId, group] of this.consumerGroups) {
            if(group.getTopicName() === name) {
                group.close()

                this.consumerGroups.delete(groupId)
            }
        }

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

    createConsumerGroup(
        groupId: string,
        topicName: string
    ) : ConsumerGroup {
        if(this.consumerGroups.has(groupId)) {
            throw new Error(`Consumer Group already exists: ${groupId}`)
        }

        const group = new ConsumerGroup(
            this,
            groupId,
            topicName
        )

        this.consumerGroups.set(groupId, group)

        return group
    }

    getConsumerGroup(groupId: string) : ConsumerGroup {
        const group = this.consumerGroups.get(groupId)

        if(!group) {
            throw new Error(`Consumer Group not found: ${groupId}`)
        }

        return group
    }

    deleteConsumerGroup(groupId: string) : boolean {
        const group = this.consumerGroups.get(groupId)

        if(!group) {
            return false
        }

        group.close()

        return this.consumerGroups.delete(groupId)
    }
}