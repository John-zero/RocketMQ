package com.learning.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 消费者
 * Created by John_zero on 2017/3/24.
 */
public class Consumer
{

    public static void main (String [] args) throws MQClientException
    {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("unique_group_name_quickstart_consumer");

        consumer.setNamesrvAddr(Constans.CONSUME_FROM_WHERE);

        /**
         * 如果需要一台服务器启动多个消费者, 则需要设置该消费者名称
         */
        consumer.setInstanceName("Consumer");

        /**
         * 注意: 一个 Consumer 可以同时订阅多个 Topic
         */

        /**
         * 订阅指定 Topic 下的所有消息
         */
        consumer.subscribe(Constans.TOPIC_STAR, Constans.TAGS_STAR);
        /**
         * 订阅指定 Topic 下的指定 Tags
         */
        consumer.subscribe(Constans.TOPIC_TAG, Constans.TAGS_TAG_0 + " || " + Constans.TAGS_TAG_1);

        consumer.registerMessageListener(new MessageListenerConcurrently()
        {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context)
            {
                System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");

                MessageExt messageExt = msgs.get(0);
                if(messageExt.getTopic().equals(Constans.TOPIC_STAR))
                {
                    System.out.println(String.format("订阅的主题: %s, Tags: %s, 消息内容: %s", Constans.TOPIC_STAR, "*", new String(messageExt.getBody())));
                }
                else if(messageExt.getTopic().equals(Constans.TOPIC_TAG))
                {
                    if(messageExt.getTags() != null)
                    {
                        switch (messageExt.getTags())
                        {
                            case Constans.TAGS_TAG_0:
                                System.out.println(String.format("订阅的主题: %s, Tags: %s, 消息内容: %s", Constans.TOPIC_TAG, Constans.TAGS_TAG_0, new String(messageExt.getBody())));
                                break;
                            case Constans.TAGS_TAG_1:
                                System.out.println(String.format("订阅的主题: %s, Tags: %s, 消息内容: %s", Constans.TOPIC_TAG, Constans.TAGS_TAG_1, new String(messageExt.getBody())));
                                break;
                            default:
                                System.out.println(String.format("订阅的主题: %s, Tags: %s, 消息内容: %s", Constans.TOPIC_TAG, "其他 Tags", new String(messageExt.getBody())));
                                break;
                        }
                    }
                    else
                    {
                        System.out.println(String.format("订阅的主题: %s, Tags: %s, 消息内容: %s", Constans.TOPIC_TAG, "无 Tags", new String(messageExt.getBody())));
                    }
                }
                else
                {
                    System.out.println(String.format("暂时未处理的主题: %s, 消息内容: %s", messageExt.getTopic(), new String(messageExt.getBody())));
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /**
         * 走起...
         */
        consumer.start();

        System.out.println("Consumer 消费者 启动完毕...");
    }

}
