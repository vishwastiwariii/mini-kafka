import type { Message } from "../types/Message.js"


export class Partition {
    private messages: Message[] = []

    private nextOffset = 0

    constructor (
        public readonly partitionId: number
    ) {}

    append(message: Omit<Message, "offset">) : Message {
        const newMessage: Message = {
            ...message, 
            offset: this.nextOffset++
        }

        return newMessage
    }

    read(offset: number) : Message | null {
        return this.messages[offset] ?? null
    }

    getMessage() : Message[] {
        return this.messages
    }

    getLatestOffset() : number {
        return this.nextOffset - 1
    }
}