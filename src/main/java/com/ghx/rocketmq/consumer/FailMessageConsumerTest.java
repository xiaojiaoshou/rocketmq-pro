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
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.TreeSet;

/**
 * MessageExtConsumer, consume listener impl class.
 */
@Service
@RocketMQMessageListener(topic = "test", consumerGroup = "string_consumer2", selectorExpression = "*")
public class FailMessageConsumerTest implements RocketMQListener<MessageExt>, RocketMQPushConsumerLifecycleListener {
    Set<Integer> set = new TreeSet<>();
    Set<String> broker = new TreeSet<>();

    @Override
    public void onMessage(MessageExt message) {

        int msg = Integer.parseInt(new String(message.getBody()));
        String ipaddress = message.getBornHostString();
        System.out.printf("------- MessageExtConsumer received message, msgId: %s, body:%s \n", message.getMsgId(), msg);
        // throw new RuntimeException("故意的异常");
        synchronized (FailMessageConsumerTest.class) {
            set.add(msg);
            broker.add(ipaddress);
            System.out.println("---------消费端总共处理消息数量set.size:" + set.size() + "brokerToString: " + broker.toString() + "message:======" + message);
        }


    }

    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        // set consumer consume message from now
//        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
//        consumer.setConsumeTimestamp(UtilAll.timeMillisToHumanString3(System.currentTimeMillis()));
    }
}
