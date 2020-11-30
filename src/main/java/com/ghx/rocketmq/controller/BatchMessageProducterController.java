package com.ghx.rocketmq.controller;

import com.ghx.rocketmq.config.MqPropertiesConfig;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @description:
 * @type: JAVA
 * @since: 2020/11/29 15:44
 * @author: guohuixiang
 */
@RequestMapping("/batch")
@RestController
public class BatchMessageProducterController {

    @Autowired
    private MqPropertiesConfig mqPropertiesConfig;

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @RequestMapping("/sync")
    public void testBatchMessages() {
        List<Message> msgs = new ArrayList<Message>();
        for (int i = 0; i < 100; i++) {
            msgs.add(MessageBuilder.withPayload("Hello RocketMQ Batch Msg#" + i).
                    setHeader(RocketMQHeaders.KEYS, "KEY_" + i).build());
        }

        SendResult sr = rocketMQTemplate.syncSend(mqPropertiesConfig.getSpringTopic(), msgs, 60000);

        System.out.printf("--- Batch messages send result :" + sr);
    }


    // DefaultMQProducer  defaultMQProducer=new DefaultMQProducer();
    //        defaultMQProducer.queryMessage()
}
