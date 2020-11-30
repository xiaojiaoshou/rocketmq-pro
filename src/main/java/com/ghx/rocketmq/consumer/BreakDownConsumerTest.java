/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ghx.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.TreeSet;

/**
 * StringConsumer
 */
@Service
@RocketMQMessageListener(topic = "test", consumerGroup = "string_consumer2", selectorExpression = "*")
public class BreakDownConsumerTest implements RocketMQListener<String>, RocketMQPushConsumerLifecycleListener {

     int count = 0;
    Set<Integer> set = new TreeSet<>();

    @Override
    public void onMessage(String message) {

        System.out.printf("------- StringConsumer received: %s \n", message);

        // throw new RuntimeException("故意的异常");
        synchronized (BreakDownConsumerTest.class) {
            count++;

            set.add(Integer.valueOf(message));
            System.out.println("---------消费端总共处理消息数量count=" + count + "message:======" + message);
        }
        System.out.println("---------消费端总共处理消息数量count=" + count + "message:======" + message);
    }


    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        // messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h，分别代表延迟level1-level18
        // 默认从 3开始 也就是  10s/30s/1m
        consumer.setMaxReconsumeTimes(2);
    }
}
