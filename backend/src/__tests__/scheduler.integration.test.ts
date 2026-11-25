/**
 * Integration tests for the scheduler worker against a REAL Redis + REAL BullMQ.
 * Prisma, SMTP (Ethereal) and Slack are faked so no Postgres / network is needed.
 *
 * WARNING: clears the `email-queue` BullMQ keys. Run against a throwaway Redis:
 *   npm run test:integration
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest';

const h = vi.hoisted(() => {
  process.env.WORKER_CONCURRENCY = '10';
  return {
    emails: new Map<string, any>(),
    sender: {
      id: 'sender-test',
      email: 'sender@test.dev',
      displayName: 'Sender',
      smtpHost: 'h',
      smtpPort: 587,
      smtpUser: 'u',
      smtpPass: 'p',
      maxEmailsPerHour: 1000,
      minDelayMsBetweenSend: 300,
    } as any,
    sends: [] as { id: string; at: number }[],
    sendImpl: null as null | ((o: any) => Promise<any>),
    slack: vi.fn(async () => true),
  };
});

vi.mock('@prisma/client', () => ({
  PrismaClient: class {},
  EmailStatus: {
    SCHEDULED: 'SCHEDULED', QUEUED: 'QUEUED', PROCESSING: 'PROCESSING', SENT: 'SENT',
    FAILED: 'FAILED', RATE_LIMITED: 'RATE_LIMITED', CANCELLED: 'CANCELLED',
  },
}));

vi.mock('../config/db', () => ({
  checkDatabaseConnection: async () => true,
  prisma: {
    email: {
      findUnique: async ({ where }: any) => {
        const e = h.emails.get(where.id);
        return e ? { ...e, sender: h.sender } : null;
      },
      updateMany: async ({ where, data }: any) => {
        const e = h.emails.get(where.id);
        if (!e || !where.status.in.includes(e.status)) return { count: 0 };
        Object.assign(e, data);
        return { count: 1 };
      },
      update: async ({ where, data }: any) => {
        const e = h.emails.get(where.id);
        Object.assign(e, data);
        return { ...e, createdAt: new Date() };
      },
    },
  },
}));

vi.mock('../services/etherealService', () => ({
  EtherealService: {
    sendEmail: async (o: any) => {
      if (h.sendImpl) return h.sendImpl(o);
      h.sends.push({ id: o.emailId, at: Date.now() });
      return { messageId: `<${o.emailId}@t>`, previewUrl: false };
    },
  },
}));
vi.mock('../services/slackService', () => ({ SlackService: { sendRateLimitNotification: h.slack } }));
vi.mock('../services/elasticsearchService', () => ({ ElasticsearchService: { indexEmail: async () => true } }));

let up = false;
try {
  const Redis = (await import('ioredis')).default;
  const r = new Redis({ host: process.env.REDIS_HOST || '127.0.0.1', lazyConnect: true, retryStrategy: () => null });
  await r.connect();
  await r.ping();
  r.disconnect();
  up = true;
} catch {
  up = false;
}

const waitFor = async (cond: () => boolean | Promise<boolean>, ms = 15000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await cond()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error('waitFor timed out');
};

describe.skipIf(!up)('scheduler worker (real Redis + BullMQ)', () => {
  let emailQueue: any, redisClient: any, worker: any;

  const addEmail = async (id: string, jobOpts: any = {}, delay = 0) => {
    h.emails.set(id, { id, status: 'SCHEDULED', scheduledAt: new Date(), createdAt: new Date() });
    await emailQueue.add(
      'send-email',
      {
        emailId: id, userId: 'u1', senderId: h.sender.id, recipient: `${id}@x.com`, subject: 's', body: 'b',
        scheduledAt: new Date().toISOString(), idempotencyKey: id,
      },
      { jobId: id, delay, attempts: 1, ...jobOpts }
    );
  };

  beforeAll(async () => {
    ({ emailQueue } = await import('../queue/emailQueue'));
    ({ redisClient } = await import('../config/redis'));
    const { createEmailWorker } = await import('../queue/worker');
    await emailQueue.obliterate({ force: true });
    worker = createEmailWorker();
  });

  beforeEach(async () => {
    h.emails.clear();
    h.sends.length = 0;
    h.sendImpl = null;
    h.slack.mockClear();
    h.sender.id = `sender-${Math.random().toString(36).slice(2)}`; // fresh Redis keys per test
    h.sender.maxEmailsPerHour = 1000;
    h.sender.minDelayMsBetweenSend = 300;
    await emailQueue.obliterate({ force: true });
    // markers/locks from previous runs would make reused test ids look "already sent"
    for (const pattern of ['email:sent:*', 'email:sending:*']) {
      const keys = await redisClient.keys(pattern);
      if (keys.length) await redisClient.del(...keys);
    }
  });

  afterAll(async () => {
    await worker?.close();
    await emailQueue?.obliterate({ force: true });
    await emailQueue?.close();
    redisClient?.disconnect();
  });

  it('min delay: 12 jobs with concurrency 10 are spaced >= minDelay apart (no burst)', async () => {
    for (let i = 0; i < 12; i++) await addEmail(`spacing-${i}`);
    await waitFor(() => h.sends.length === 12, 20000);
    const times = h.sends.map((s) => s.at).sort((a, b) => a - b);
    const gaps = times.slice(1).map((t, i) => t - times[i]);
    expect(Math.min(...gaps)).toBeGreaterThanOrEqual(270); // 300ms minus timer jitter
    expect(h.sends.every((s) => h.emails.get(s.id).status === 'SENT')).toBe(true);
  });

  it('hourly limit: over-limit job is DELAYED (not failed/dropped), DB marked RATE_LIMITED, Slack notified once', async () => {
    h.sender.maxEmailsPerHour = 2;
    h.sender.minDelayMsBetweenSend = 50;
    for (let i = 0; i < 4; i++) await addEmail(`rl-${i}`);
    await waitFor(() => h.sends.length === 2);
    await waitFor(() => ['rl-0', 'rl-1', 'rl-2', 'rl-3'].filter((id) => h.emails.get(id).status === 'RATE_LIMITED').length === 2);
    await new Promise((r) => setTimeout(r, 300));

    expect(h.sends.length).toBe(2);
    const limited = ['rl-0', 'rl-1', 'rl-2', 'rl-3'].filter((id) => h.emails.get(id).status === 'RATE_LIMITED');
    for (const id of limited) {
      const job = await emailQueue.getJob(id);
      expect(await job.getState()).toBe('delayed');
      const nextHour = Math.ceil(Date.now() / 3600000) * 3600000;
      expect(h.emails.get(id).scheduledAt.getTime()).toBeGreaterThanOrEqual(nextHour);
      expect(h.emails.get(id).scheduledAt.getTime()).toBeLessThan(nextHour + 1000);
    }
    expect(await emailQueue.getFailedCount()).toBe(0);
    expect(h.slack).toHaveBeenCalledTimes(1);
  });

  it('ordering: over-limit jobs are rescheduled into the next window in their original order', async () => {
    h.sender.maxEmailsPerHour = 1;
    h.sender.minDelayMsBetweenSend = 100;
    for (let i = 0; i < 4; i++) {
      await addEmail(`ord-${i}`, {}, i * 150); // staggered pickup like a real campaign
    }
    await waitFor(() => ['ord-1', 'ord-2', 'ord-3'].every((id) => h.emails.get(id).status === 'RATE_LIMITED'), 20000);
    const t = [1, 2, 3].map((i) => h.emails.get(`ord-${i}`).scheduledAt.getTime());
    expect(t[0]).toBeLessThan(t[1]);
    expect(t[1]).toBeLessThan(t[2]);
  });

  it('retry: SMTP failure is retried (stays QUEUED, not FAILED) and eventually SENT exactly once', async () => {
    let calls = 0;
    h.sendImpl = async (o) => {
      calls++;
      if (calls === 1) throw new Error('smtp boom');
      h.sends.push({ id: o.emailId, at: Date.now() });
      return { messageId: 'm', previewUrl: false };
    };
    await addEmail('retry-1', { attempts: 3, backoff: { type: 'fixed', delay: 200 } });
    await waitFor(() => calls >= 1 && h.emails.get('retry-1').failureReason === 'smtp boom');
    expect(['QUEUED', 'PROCESSING', 'SENT']).toContain(h.emails.get('retry-1').status);
    await waitFor(() => h.emails.get('retry-1').status === 'SENT');
    expect(calls).toBe(2);
    expect(h.sends.length).toBe(1);
  });

  it('retry: becomes FAILED only after the final attempt', async () => {
    let calls = 0;
    h.sendImpl = async () => {
      calls++;
      throw new Error('always down');
    };
    await addEmail('fail-1', { attempts: 2, backoff: { type: 'fixed', delay: 100 } });
    await waitFor(() => h.emails.get('fail-1').status === 'FAILED');
    expect(calls).toBe(2);
  });

  it('idempotency: already SENT email is never re-sent', async () => {
    h.emails.set('sent-1', { id: 'sent-1', status: 'SENT', scheduledAt: new Date(), createdAt: new Date() });
    await emailQueue.add('send-email', { emailId: 'sent-1', userId: 'u1', senderId: h.sender.id, recipient: 'a@b.c', subject: 's', body: 'b', scheduledAt: new Date().toISOString(), idempotencyKey: 'k' }, { jobId: 'sent-1' });
    await waitFor(async () => (await emailQueue.getCompletedCount()) === 1);
    expect(h.sends.length).toBe(0);
  });

  it('idempotency: send succeeded but DB update was lost -> marker reconciles DB, no duplicate send', async () => {
    await redisClient.set('email:sent:lost-1', 'preview-url', 'EX', 60);
    h.emails.set('lost-1', { id: 'lost-1', status: 'PROCESSING', scheduledAt: new Date(), createdAt: new Date() });
    await emailQueue.add('send-email', { emailId: 'lost-1', userId: 'u1', senderId: h.sender.id, recipient: 'a@b.c', subject: 's', body: 'b', scheduledAt: new Date().toISOString(), idempotencyKey: 'k2' }, { jobId: 'lost-1' });
    await waitFor(() => h.emails.get('lost-1').status === 'SENT');
    expect(h.sends.length).toBe(0);
    expect(h.emails.get('lost-1').providerMessageId).toBe('preview-url');
  });

  it('idempotency: re-adding the same jobId does not create a second job / second send', async () => {
    await addEmail('dup-1', {}, 500);
    await addEmail('dup-1', {}, 500); // same jobId -> BullMQ ignores
    await waitFor(() => h.sends.length >= 1);
    await new Promise((r) => setTimeout(r, 600));
    expect(h.sends.filter((s) => s.id === 'dup-1').length).toBe(1);
  });

  it('send lock: a concurrent in-flight send of the same email makes the job wait instead of double-sending', async () => {
    await redisClient.set('email:sending:lock-1', 'other-worker', 'PX', 1500);
    await addEmail('lock-1');
    await new Promise((r) => setTimeout(r, 600));
    expect(h.sends.length).toBe(0);
    await waitFor(() => h.sends.length === 1, 25000); // proceeds once the lock expires (re-check every 15s)
  }, 40000);
});
