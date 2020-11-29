package com.ghx.rocketmq.controller;

import com.ghx.rocketmq.config.MqPropertiesConfig;
import com.ghx.rocketmq.domain.User;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 *
 * @description:
 * @type: JAVA
 * @since: 2020/11/29 9:58
 * @author: guohuixiang
 */
@RequestMapping("/producter")
@RestController
public class ProducterController {
    @Autowired
    private MqPropertiesConfig mqPropertiesConfig;

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    /**
     * 发送字符串
     * @param message
     * @return
     */
    @RequestMapping(value = "/syncSend-string")
    public SendResult sendString(String message) {
        // Send string
        SendResult sendResult = rocketMQTemplate.syncSend(mqPropertiesConfig.getSpringTopic(), message);
        System.out.printf("syncSend1 to topic %s sendResult=%s %n", mqPropertiesConfig.getSpringTopic(), sendResult);
        return sendResult;
    }

    /**
     *  发送字符串
     * @param message
     * @return
     */
    @RequestMapping(value = "/syncSend-Spring-String")
    public SendResult sendSpringString(String message) {
        // Send string with spring Message
        SendResult sendResult = rocketMQTemplate.syncSend(mqPropertiesConfig.getSpringTopic(), MessageBuilder.withPayload(message).build());
        System.out.printf("syncSend2 to topic %s sendResult=%s %n", mqPropertiesConfig.getSpringTopic(), sendResult);
        return sendResult;
    }

    /**
     *  发送字符串
     * @param message
     * @return
     */
    @RequestMapping(value = "/syncSend-sendMsgWithTag")
    public SendResult sendMsgWithTag(String message) {
        // Send message with special tag
        // tag0 will not be consumer-selected
        rocketMQTemplate.convertAndSend(mqPropertiesConfig.getMsgExtTopic() + ":tag0", "I'm from tag0");
        System.out.printf("syncSend topic %s tag %s %n", mqPropertiesConfig.getMsgExtTopic(), "tag0");
        rocketMQTemplate.convertAndSend(mqPropertiesConfig.getMsgExtTopic() + ":tagA", "I'm from tagA");
        System.out.printf("syncSend topic %s tag %s %n", mqPropertiesConfig.getMsgExtTopic(), "tagA");
        return null;
    }

    /**
     * 发送json
     * @param user
     * @return
     */
    @RequestMapping(value = "/syncSend-json")
    public SendResult sendJson(@RequestBody User user) {
        String userTopic = mqPropertiesConfig.getUserTopic();
        SendResult sendResult = rocketMQTemplate.syncSend(userTopic, MessageBuilder.withPayload(
                user).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE).build());
        System.out.printf("syncSend1 to topic %s sendResult=%s %n", userTopic, sendResult);
        return sendResult;
    }


}
