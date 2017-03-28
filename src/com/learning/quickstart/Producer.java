package com.learning.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.CountDownLatch;

/**
 * 生产者
 * Created by John_zero on 2017/3/24.
 */
public class Producer
{

    public static void main (String [] args) throws MQClientException, InterruptedException
    {
        DefaultMQProducer producer = new DefaultMQProducer("unique_group_name_quickstart_producer");

        producer.setNamesrvAddr(Constans.PRODUCER_TO_WHERE);

        /**
         * 如果需要一台服务器启动多个生产者, 则需要设置该生产者名称
         */
        producer.setInstanceName("Producer");

        producer.setVipChannelEnabled(false);

        /**
         * 走起...
         */
        producer.start();

        System.out.println("Producer 生产者 启动完毕...");

        /**
         * 模拟生产消息
         */

        CountDownLatch countDownLatch = new CountDownLatch(15);

        /**
         * TOPIC_STAR, TAGS_STAR
         */
        new Thread(() -> {
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    String messageContent = String.format("Hello, Topic: %s, Tags: %s, Index: %s, Message", Constans.TOPIC_STAR, Constans.TAGS_STAR, i);
                    Message message = new Message(Constans.TOPIC_STAR, Constans.TAGS_STAR,"quickstart_topic_star_tags_star_" + i, messageContent.getBytes());
                    SendResult sendResult = producer.send(message);
                    System.out.println(String.format("Message, Topic: %s, Tags: %s, Index: %s, SendResult: %s", Constans.TOPIC_STAR, Constans.TAGS_STAR, i, sendResult));
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                finally
                {
                    countDownLatch.countDown();
                }
            }
        }).start();

        /**
         * TOPIC_TAG, TAGS_TAG_0
         */
        new Thread(() -> {
            for (int i = 100; i < 105; i++)
            {
                try
                {
                    String messageContent = String.format("Hello, Topic: %s, Tags: %s, Index: %s, Message", Constans.TOPIC_TAG, Constans.TAGS_TAG_0, i);
                    Message message = new Message(Constans.TOPIC_TAG, Constans.TAGS_TAG_0,"quickstart_topic_star_tags_star_" + i, messageContent.getBytes());
                    SendResult sendResult = producer.send(message);
                    System.out.println(String.format("Message, Topic: %s, Tags: %s, Index: %s, SendResult: %s", Constans.TOPIC_TAG, Constans.TAGS_TAG_0, i, sendResult));
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                finally
                {
                    countDownLatch.countDown();
                }
            }
        }).start();

        /**
         * TOPIC_TAG, TAGS_TAG_1
         */
        new Thread(() -> {
            for (int i = 200; i < 205; i++)
            {
                try
                {
                    String messageContent = String.format("Hello, Topic: %s, Tags: %s, Index: %s, Message", Constans.TOPIC_TAG, Constans.TAGS_TAG_1, i);
                    Message message = new Message(Constans.TOPIC_TAG, Constans.TAGS_TAG_1,"quickstart_topic_star_tags_star_" + i, messageContent.getBytes());
                    SendResult sendResult = producer.send(message);
                    System.out.println(String.format("Message, Topic: %s, Tags: %s, Index: %s, SendResult: %s", Constans.TOPIC_TAG, Constans.TAGS_TAG_1, i, sendResult));
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                finally
                {
                    countDownLatch.countDown();
                }
            }
        }).start();

        countDownLatch.await();

        /**
         * 清除痕迹
         */
        producer.shutdown();
    }

}
