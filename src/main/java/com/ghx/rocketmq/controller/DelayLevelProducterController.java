package com.ghx.rocketmq.controller;

import com.alibaba.fastjson.TypeReference;
import com.ghx.rocketmq.config.MqPropertiesConfig;
import com.ghx.rocketmq.domain.ProductWithPayload;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @description:
 * @type: JAVA
 * @since: 2020/11/29 14:54
 * @author: guohuixiang
 */
@RestController
@RequestMapping("/delayLevel")
public class DelayLevelProducterController {
    @Autowired
    private MqPropertiesConfig mqPropertiesConfig;

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @RequestMapping("/string")
    public SendResult test(String message) {

        //创建 Demo03Message 消息
        Message msg = MessageBuilder.withPayload(message)
                .build();
        // 同步发送消息
        SendResult sendResult = rocketMQTemplate.syncSend(mqPropertiesConfig.getGenericRequestTopic(), msg, 30 * 1000,
                4);
        System.out.printf("sendResult %s  %n", "request generic", sendResult);
        return sendResult;

    }


    @RequestMapping("/string2")
    public void test1(String message) {
        // Send request in sync mode with timeout and delayLevel parameter parameter and receive a reply of generic type.
        ProductWithPayload<String> replyGenericObject = rocketMQTemplate.sendAndReceive("genericRequestTopic2:tagA", message,
                new TypeReference<ProductWithPayload<String>>() {
                }.getType(), 300000, 2);
        System.out.printf("send %s and receive %s %n", "request generic", replyGenericObject);

    }
}
