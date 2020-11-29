package com.ghx.rocketmq.controller;

import com.ghx.rocketmq.config.MqPropertiesConfig;
import com.ghx.rocketmq.domain.User;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
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
     *
     * %RETRY%消费组名称 重投队列
     * %DLQ%消费组名称  死信队列
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


    /**
     * 发送异步消息
     * @param user
     * @return
     */
    @RequestMapping(value = "/asyncSend")
    public String asyncSend(@RequestBody User user) {
        String userTopic = mqPropertiesConfig.getUserTopic();
        Message<User> message = MessageBuilder.withPayload(user)
                .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE).build();
        rocketMQTemplate.asyncSend(userTopic, message, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.printf("syncSend1 to topic sendResult=%s %n", sendResult);
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                System.out.printf("syncSend1 to topic fail,e=%s %n", e);
            }
        });

        return "发送消息成功";
    }

    /**
     * 请求应答
     * @return
     */
    @RequestMapping("/sendAndReceive")
    public String sendAndReceive(String message) {
        String stringRequestTopic = mqPropertiesConfig.getStringRequestTopic();

        // Send request in sync mode and receive a reply of String type.
        String replyString = rocketMQTemplate.sendAndReceive(stringRequestTopic, message, String.class);
        System.out.printf("send %s and receive %s %n", "request string", replyString);
        return replyString;
    }


}
