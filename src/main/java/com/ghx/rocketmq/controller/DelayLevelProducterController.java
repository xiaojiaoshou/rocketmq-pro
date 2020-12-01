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
        // messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h，分别代表延迟level1-level18
        SendResult sendResult = rocketMQTemplate.syncSend(mqPropertiesConfig.getGenericRequestTopic(), msg, 30 * 1000,
                3);
        System.out.printf("生产端:sendResult %s  %n", "request generic", sendResult);
        return sendResult;

    }


    @RequestMapping("/string2")
    public void test1(String message) {
        // Send request in sync mode with timeout and delayLevel parameter parameter and receive a reply of generic type.
         // messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h，分别代表延迟level1-level18
        ProductWithPayload<String> replyGenericObject = rocketMQTemplate.sendAndReceive("genericRequestTopic2:tagA", message,
                new TypeReference<ProductWithPayload<String>>() {
                }.getType(), 300000, 1);
        System.out.printf("生产端:send %s and receive %s %n", "request generic", replyGenericObject);

    }
}
