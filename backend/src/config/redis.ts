import Redis, { RedisOptions } from 'ioredis';
import { config } from './env';
import { logger } from '../utils/logger';

const redisUrl = process.env.REDIS_URL;
const redisPassword = process.env.REDIS_PASSWORD;

const baseOptions: RedisOptions = {
  maxRetriesPerRequest: null, // Required by BullMQ
  enableReadyCheck: false,
  ...(redisPassword ? { password: redisPassword } : {}),
  ...(process.env.REDIS_TLS === 'true' ? { tls: {} } : {}),
};

// BullMQ needs connection *options* (a raw URL string is not valid), so a REDIS_URL is parsed into options.
function buildRedisOptions(): RedisOptions {
  if (redisUrl) {
    const u = new URL(redisUrl);
    return {
      host: u.hostname,
      port: u.port ? parseInt(u.port, 10) : 6379,
      ...(u.username ? { username: decodeURIComponent(u.username) } : {}),
      ...(u.password ? { password: decodeURIComponent(u.password) } : {}),
      ...(u.protocol === 'rediss:' ? { tls: {} } : {}),
      maxRetriesPerRequest: null,
      enableReadyCheck: false,
    };
  }
  return { host: config.redisHost, port: config.redisPort, ...baseOptions };
}

export const redisConnectionOptions: RedisOptions = buildRedisOptions();

export const redisClient = new Redis(redisConnectionOptions);

redisClient.on('connect', () => {
  logger.info(`Redis connected successfully (${redisUrl ? 'REDIS_URL' : `${config.redisHost}:${config.redisPort}`})`);
});

redisClient.on('error', (err) => {
  logger.error('Redis connection error:', err);
});

export async function checkRedisConnection(): Promise<boolean> {
  try {
    const ping = await redisClient.ping();
    return ping === 'PONG';
  } catch (err) {
    logger.error('Redis health check failed:', err);
    return false;
  }
}
