import type { Message } from "../types/Message.js"


export class Partition {
    private messages: Message[] = []

    private nextOffset = 0

    constructor (
        public readonly partitionId: number
    ) {
        if(partitionId < 0) {
            throw new Error("Invalid partitionId")
        }
    }

    append(message: Omit<Message, "offset">) : Message {
        const newMessage: Message = Object.freeze({
            ...message, 
            offset: this.nextOffset
        })

        this.messages.push(newMessage)

        this.nextOffset++

        return newMessage
    }

    read(offset: number) : Message | null {
        if(!Number.isInteger(offset) || offset < 0) {
            return null
        }

        return this.messages[offset] ?? null
    }

    getMessages() : Message[] {
        const messages = [...this.messages]

        return messages
    }

    getLatestOffset() : number {
        return this.nextOffset - 1
    }
}