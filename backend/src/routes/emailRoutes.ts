import { Router } from 'express';
import { EmailController } from '../controllers/emailController';
import { authMiddleware } from '../middleware/authMiddleware';

const router = Router();

router.use(authMiddleware);

router.post('/schedule', EmailController.scheduleEmails);
router.get('/scheduled', EmailController.getScheduledEmails);
router.get('/sent', EmailController.getSentEmails);
router.get('/search', EmailController.searchEmails);
router.get('/senders', EmailController.listSenders);
router.post('/senders', EmailController.createSender);

export default router;