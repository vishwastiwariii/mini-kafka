export interface Message {
    id: string;
    topic: string;
    partition: number;
    offset: number;
    key?: string | undefined;
    value: any;
    timestamp: number;
}
//# sourceMappingURL=Message.d.ts.map