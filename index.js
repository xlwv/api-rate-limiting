import express from 'express';
import { RateLimiterRedis } from 'rate-limiter-flexible';
import Redis from 'ioredis';
import fs from 'fs';
import Queue from 'bull';
import dotenv from 'dotenv';

dotenv.config();
const app = express();
app.use(express.json());

const redisClient = new Redis();

redisClient.on('error', (err) => {
  console.error('Redis Client Error', err);
});


const rateLimiter = new RateLimiterRedis({
  storeClient: redisClient,
  keyPrefix: 'rateLimiter',
  points: 1, // Number of points
  duration: 1, // Per second
  blockDuration: 60, // Block for 60 seconds if more than points consumed
});

const minuteLimiter = new RateLimiterRedis({
  storeClient: redisClient,
  keyPrefix: 'minuteLimiter',
  points: 3, // Number of points
  duration: 60, // Per minute
});


const taskQueue = new Queue('taskQueue', { redis: { host: process.env.REDIS_HOST, port: process.env.REDIS_PORT} });

// Task function
async function task(user_id) {
  
  const logMessage = `${user_id} - task completed at - ${new Date().toISOString()}\n`;
  console.log(logMessage);
 
  fs.appendFile('task.log', logMessage, err => {
    if (err) console.error('Error writing to log file', err);
  });
}

// Process the task queue
taskQueue.process(async (job) => {
  const { user_id } = job.data;
  await task(user_id);
});

app.post('/task', async (req, res) => {
  const { user_id } = req.body;

  try {
    await rateLimiter.consume(user_id); 
    await minuteLimiter.consume(user_id); 

    // Add task to the queue
    taskQueue.add({ user_id });

    res.status(200).send('Task queued.');
  } catch (rejRes) {
    if (rejRes instanceof Error) {
      console.error('Error processing task:', rejRes);
      res.status(500).send('Internal Server Error');
    } else {
      // If rate limit exceeded, queue the task but delay it
      const delay = rejRes.msBeforeNext;
      taskQueue.add({ user_id }, { delay });
      res.status(429).send('Rate limit exceeded. ');
    }
  }
});



const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
