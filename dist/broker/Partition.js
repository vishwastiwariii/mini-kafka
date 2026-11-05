export class Partition {
    partitionId;
    messages = [];
    nextOffset = 0;
    constructor(partitionId) {
        this.partitionId = partitionId;
    }
    append(message) {
        const newMessage = {
            ...message,
            offset: this.nextOffset++
        };
        return newMessage;
    }
    read(offset) {
        return this.messages[offset] ?? null;
    }
    getMessage() {
        return this.messages;
    }
    getLatestOffset() {
        return this.nextOffset - 1;
    }
}
//# sourceMappingURL=Partition.js.map