import { redisClient } from '../config/redis';
import { logger } from '../utils/logger';

const WINDOW_MS = 60 * 60 * 1000; // fixed UTC-hour windows

/**
 * Atomic "slot reservation" per sender. One script enforces BOTH:
 *  - minimum spacing between sends (next-slot pointer advances by minDelay per reservation), and
 *  - the hourly limit (per-window counter; a full window pushes the slot to the start of the next window).
 * Because it runs atomically in Redis it is safe across any number of workers/instances, and because the
 * pointer only moves forward, slots are handed out in reservation order (ordering is preserved).
 *
 * KEYS[1] = next-slot pointer key. ARGV: nowMs, minDelayMs, hourlyLimit, windowMs, counterKeyPrefix
 * Returns { slotAtMs, limitedFlag(1 if a full hourly window was skipped), countInSlotWindow }
 */
const RESERVE_SLOT_LUA_SCRIPT = `
local now = tonumber(ARGV[1])
local min_delay = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local win = tonumber(ARGV[4])
local prefix = ARGV[5]

local next_slot = tonumber(redis.call("GET", KEYS[1]) or "0")
local slot = math.max(now, next_slot)
local limited = 0
local count = 0

while true do
  local w = slot - (slot % win)
  local ckey = prefix .. string.format("%d", w)
  count = tonumber(redis.call("GET", ckey) or "0")
  if count < limit then
    count = redis.call("INCR", ckey)
    redis.call("PEXPIRE", ckey, (w + 2 * win) - now)
    break
  end
  limited = 1
  slot = w + win
end

redis.call("SET", KEYS[1], string.format("%d", slot + min_delay), "PX", (slot - now) + min_delay + 86400000)
return { string.format("%d", slot), limited, count }
`;

export interface SlotReservation {
  /** Epoch ms at which this send is allowed to happen. */
  slotAt: number;
  /** True when the sender's current hourly window was full and the send was pushed to a later window. */
  limited: boolean;
  /** Number of sends now reserved in the slot's hourly window. */
  windowCount: number;
}

export class RateLimiterService {
  /**
   * Generates a key for the current hourly window.
   * Format: YYYYMMDDHH
   */
  private static getHourlyWindowKey(senderId: string, date: Date = new Date()): string {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, '0');
    const day = String(date.getUTCDate()).padStart(2, '0');
    const hour = String(date.getUTCHours()).padStart(2, '0');
    return `rate_limit:sender:${senderId}:${year}${month}${day}${hour}`;
  }

  /**
   * Calculates milliseconds remaining until the start of the next UTC hour.
   */
  public static getMsUntilNextHour(now: Date = new Date()): number {
    const nextHour = new Date(now);
    nextHour.setUTCHours(now.getUTCHours() + 1, 0, 0, 0);
    return Math.max(1000, nextHour.getTime() - now.getTime());
  }

  /**
   * Atomically reserves the next send slot for a sender (min-delay spacing + hourly limit).
   * Throws if Redis is unavailable so the job is retried instead of bypassing the limits.
   */
  public static async reserveSlot(
    senderId: string,
    maxHourlyLimit: number,
    minDelayMs: number,
    now: number = Date.now()
  ): Promise<SlotReservation> {
    const [slot, limited, count] = (await redisClient.eval(
      RESERVE_SLOT_LUA_SCRIPT,
      1,
      `sender:next_slot:${senderId}`,
      now,
      Math.max(0, minDelayMs),
      Math.max(1, maxHourlyLimit),
      WINDOW_MS,
      `rate_limit:sender:${senderId}:w:`
    )) as [string, number, number];

    const reservation = { slotAt: parseInt(slot, 10), limited: limited === 1, windowCount: count };
    logger.debug(
      `Slot reserved for sender ${senderId}: in ${reservation.slotAt - now}ms, limited=${reservation.limited}, windowCount=${count}/${maxHourlyLimit}`
    );
    return reservation;
  }

  /**
   * Helper to check if Slack has already been notified for this sender in this hour window.
   */
  public static async shouldNotifySlack(senderId: string): Promise<boolean> {
    const windowKey = this.getHourlyWindowKey(senderId);
    const notificationKey = `slack_notified:${windowKey}`;

    try {
      // SETNX (set if not exists) with 1 hour TTL
      const setSuccess = await redisClient.set(notificationKey, '1', 'EX', 3600, 'NX');
      return setSuccess === 'OK';
    } catch (error) {
      logger.error('Error checking Slack notification deduplication:', error);
      return false;
    }
  }
}
