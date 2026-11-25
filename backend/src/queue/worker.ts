import { Worker, Job, DelayedError } from 'bullmq';
import { EMAIL_QUEUE_NAME, EmailJobPayload } from './emailQueue';
import { redisConnectionOptions, redisClient } from '../config/redis';
import { prisma } from '../config/db';
import { EmailStatus } from '@prisma/client';
import { RateLimiterService } from '../services/rateLimiterService';
import { EtherealService } from '../services/etherealService';
import { SlackService } from '../services/slackService';
import { ElasticsearchService } from '../services/elasticsearchService';
import { config } from '../config/env';
import { logger } from '../utils/logger';

export function createEmailWorker(): Worker<EmailJobPayload> {
  const worker = new Worker<EmailJobPayload>(
    EMAIL_QUEUE_NAME,
    async (job: Job<EmailJobPayload>, token?: string) => {
      const { emailId, userId, senderId, recipient, subject, body, scheduledAt, idempotencyKey } =
        job.data;

      logger.info(
        `[Worker Thread ${process.pid}] Processing job ${job.id} for Email ID: ${emailId} -> Recipient: ${recipient}`
      );

      // 1. Fetch Email and Sender from PostgreSQL
      const emailRecord = await prisma.email.findUnique({
        where: { id: emailId },
        include: { sender: true },
      });

      if (!emailRecord) {
        logger.warn(`Email record ${emailId} not found in database. Skipping job.`);
        return { status: 'CANCELLED_NOT_FOUND' };
      }

      // Idempotency guard A: an earlier attempt already sent this email but the DB update was lost.
      // The Redis marker is written immediately after the SMTP call succeeds, before the DB update.
      const sentMarker = await redisClient.get(`email:sent:${emailId}`);
      if (sentMarker) {
        if (emailRecord.status !== EmailStatus.SENT) {
          await prisma.email.update({
            where: { id: emailId },
            data: { status: EmailStatus.SENT, sentAt: new Date(), providerMessageId: sentMarker, failureReason: null },
          });
        }
        logger.info(`Email ${emailId} already sent (provider marker found). Skipping duplicate send.`);
        return { status: 'SKIPPED_ALREADY_SENT' };
      }

      // Idempotency guard B: terminal states are never re-processed.
      if (
        emailRecord.status === EmailStatus.SENT ||
        emailRecord.status === EmailStatus.FAILED ||
        emailRecord.status === EmailStatus.CANCELLED
      ) {
        logger.info(`Email ${emailId} is already ${emailRecord.status}. Skipping.`);
        return { status: `SKIPPED_${emailRecord.status}` };
      }

      // 2. Atomic claim of any non-terminal state. PROCESSING is included on purpose: BullMQ's per-job lock
      //    guarantees only one worker runs this job at a time, so PROCESSING here means a previous attempt
      //    of THIS job (retry / re-delay / crash recovery), not a concurrent duplicate.
      const claimed = await prisma.email.updateMany({
        where: {
          id: emailId,
          status: {
            in: [EmailStatus.SCHEDULED, EmailStatus.QUEUED, EmailStatus.RATE_LIMITED, EmailStatus.PROCESSING],
          },
        },
        data: { status: EmailStatus.PROCESSING },
      });
      if (claimed.count === 0) {
        logger.warn(`Email ${emailId} reached a terminal state while claiming. Skipping.`);
        return { status: 'SKIPPED_STATE_CHANGED' };
      }

      const sender = emailRecord.sender;
      const maxHourlyLimit = sender.maxEmailsPerHour ?? config.defaultHourlyLimit;
      const minDelayMs = sender.minDelayMsBetweenSend ?? config.defaultMinDelayMs;

      // 3. Reserve a send slot (atomic in Redis: min-delay spacing + hourly limit, safe across workers).
      //    A job that already holds a reservation (it was re-delayed to its slot) must not reserve again.
      let slotAt = job.data.reservedSlotAt;
      let limited = false;
      if (slotAt === undefined) {
        const reservation = await RateLimiterService.reserveSlot(senderId, maxHourlyLimit, minDelayMs);
        slotAt = reservation.slotAt;
        limited = reservation.limited;
        if (slotAt > Date.now()) {
          await job.updateData({ ...job.data, reservedSlotAt: slotAt });
        }
      }

      if (slotAt > Date.now()) {
        const slotDate = new Date(slotAt);
        // Anything pushed into a later hourly window is "rate limited" (not just the first job that hit the cap).
        const pushedToLaterWindow = Math.floor(slotAt / 3600000) > Math.floor(Date.now() / 3600000);
        const isRateLimited = limited || pushedToLaterWindow;
        logger.info(
          `Job ${job.id} for ${sender.email} deferred to ${slotDate.toISOString()} (${isRateLimited ? `hourly limit ${maxHourlyLimit}/hr reached` : 'min-delay spacing'})`
        );

        await prisma.email.update({
          where: { id: emailId },
          data: { status: isRateLimited ? EmailStatus.RATE_LIMITED : EmailStatus.QUEUED, scheduledAt: slotDate },
        });

        if (limited && (await RateLimiterService.shouldNotifySlack(senderId))) {
          SlackService.sendRateLimitNotification(userId, sender.email, maxHourlyLimit, slotDate.toISOString()).catch(
            (err) => logger.error('Background Slack notify error:', err)
          );
        }

        // Correct BullMQ pattern: move to delayed, then throw DelayedError so the worker does not
        // try to complete/fail the job. Nothing is dropped and no attempt is consumed.
        await job.moveToDelayed(slotAt, token);
        throw new DelayedError();
      }

      // 4. Per-email send lock (defends against a stalled job being picked up while the first run is mid-send)
      const sendLockKey = `email:sending:${emailId}`;
      const gotLock = await redisClient.set(sendLockKey, token || '1', 'PX', 120000, 'NX');
      if (gotLock !== 'OK') {
        logger.warn(`Email ${emailId} is currently being sent by another run. Re-checking in 15s.`);
        await job.moveToDelayed(Date.now() + 15000, token);
        throw new DelayedError();
      }

      // 5. Execute SMTP send via Ethereal Mail
      let sendResult;
      try {
        sendResult = await EtherealService.sendEmail({
          emailId,
          fromEmail: sender.email,
          fromName: sender.displayName,
          to: recipient,
          subject,
          body,
          smtpHost: sender.smtpHost,
          smtpPort: sender.smtpPort,
          smtpUser: sender.smtpUser,
          smtpPass: sender.smtpPass,
        });
      } catch (sendError: any) {
        await redisClient.del(sendLockKey);
        logger.error(`SMTP sending error for email ${emailId}:`, sendError);

        // FAILED only after the final attempt; otherwise keep it retryable (QUEUED).
        const maxAttempts = job.opts.attempts ?? 1;
        const isFinalAttempt = job.attemptsMade + 1 >= maxAttempts;
        await prisma.email.update({
          where: { id: emailId },
          data: {
            status: isFinalAttempt ? EmailStatus.FAILED : EmailStatus.QUEUED,
            failureReason: sendError.message || 'SMTP send failed',
          },
        });

        // Retry must reserve a fresh slot so rate limits/spacing still apply.
        const { reservedSlotAt: _released, ...dataWithoutSlot } = job.data;
        await job.updateData(dataWithoutSlot as EmailJobPayload);
        throw sendError; // triggers BullMQ retry/backoff (or final failure)
      }

      // 6. Record the send in Redis FIRST (cheap, survives a DB failure), then persist to PostgreSQL.
      const providerId = typeof sendResult.previewUrl === 'string' ? sendResult.previewUrl : sendResult.messageId;
      await redisClient.set(`email:sent:${emailId}`, providerId, 'EX', 7 * 24 * 3600);
      await redisClient.del(sendLockKey);

      const sentAt = new Date();
      const updatedEmail = await prisma.email.update({
        where: { id: emailId },
        data: {
          status: EmailStatus.SENT,
          sentAt,
          providerMessageId: providerId,
          failureReason: null,
        },
      });

      // 7. Sync update to Elasticsearch
      ElasticsearchService.indexEmail({
        id: emailId,
        userId,
        senderId,
        senderEmail: sender.email,
        recipient,
        subject,
        body,
        status: EmailStatus.SENT,
        scheduledAt,
        sentAt: sentAt.toISOString(),
        createdAt: updatedEmail.createdAt.toISOString(),
      }).catch((err) => logger.warn('Non-blocking ES index update warning:', err));

      logger.info(`[SUCCESS] Email ${emailId} sent to ${recipient}. Ethereal Preview: ${sendResult.previewUrl || 'N/A'}`);

      return { status: 'SENT', messageId: sendResult.messageId, previewUrl: sendResult.previewUrl };
    },
    {
      connection: redisConnectionOptions,
      concurrency: config.workerConcurrency,
    }
  );

  worker.on('completed', (job) => {
    logger.debug(`Job ${job.id} completed successfully`);
  });

  worker.on('failed', (job, err) => {
    logger.error(`Job ${job?.id} failed with error:`, err);
  });

  return worker;
}
