import type { Message } from "../types/Message.js";
export declare class Partition {
    readonly partitionId: number;
    private messages;
    private nextOffset;
    constructor(partitionId: number);
    append(message: Omit<Message, "offset">): Message;
    read(offset: number): Message | null;
    getMessage(): Message[];
    getLatestOffset(): number;
}
//# sourceMappingURL=Partition.d.ts.map