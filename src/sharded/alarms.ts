/**
 * An alarm this long past due is taken for stranded: the platform can leave `getAlarm()`
 * answering a time in the past for an alarm that will never fire. A running alarm reads as null,
 * and one due a moment ago is about to run, so only an alarm well past due is suspect.
 */
export const STRANDED_ALARM_MS = 5 * 60_000;

/**
 * The time of the alarm set on `storage` that will still fire, or null: an alarm more than
 * STRANDED_ALARM_MS past due counts as missing. Every "is an alarm set already?" check asks this,
 * or the work the alarm serves stops for good while calls keep arriving.
 */
export async function pendingAlarm(storage: DurableObjectStorage): Promise<number | null> {
  const at = await storage.getAlarm();
  return at !== null && at < Date.now() - STRANDED_ALARM_MS ? null : at;
}
