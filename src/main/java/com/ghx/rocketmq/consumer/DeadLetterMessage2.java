package com.ghx.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.stereotype.Service;

/**
 *
 * @description:
 * @type: JAVA
 * @since: 2020/11/29 17:39
 * @author: guohuixiang
 */
@Service
@RocketMQMessageListener(topic = "%DLQ%deadLetter_consumer_test", consumerGroup = "deadLetter_consumer_test2", selectorExpression = "*")
public class DeadLetterMessage2 implements RocketMQListener<String>, RocketMQPushConsumerLifecycleListener {
    @Override
    public void onMessage(String message) {

        System.out.printf("消费端------- StringConsumer received: %s \n", message);

       // throw new RuntimeException("故意的异常");
    }


    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        // messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h，分别代表延迟level1-level18
        // 默认从 3开始 也就是  10s/30s/1m
        consumer.setMaxReconsumeTimes(2);
    }
}
