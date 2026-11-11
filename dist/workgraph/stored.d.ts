/** The largest result or value kept here, encoded: bulk data belongs in the app's storage. */
export declare const MAX_STORED_BYTES: number;
/** The store's key of a unit's result. */
export declare const resultKey: (hash: string) => string;
/** The store's key of a unit's exported value. */
export declare const valueKey: (hash: string) => string;
/** JSON, with Uint8Arrays kept as bytes. */
export declare function encodeStored(value: unknown, what: string): string;
export declare function decodeStored<T>(text: string): T;
//# sourceMappingURL=stored.d.ts.map