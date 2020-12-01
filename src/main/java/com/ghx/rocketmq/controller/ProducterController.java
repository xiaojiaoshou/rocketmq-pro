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
        System.out.printf("生产端:syncSend to topic %s sendResult=%s %n", mqPropertiesConfig.getSpringTopic(), sendResult);
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
        System.out.printf("生产端:syncSend to topic %s sendResult=%s %n", mqPropertiesConfig.getSpringTopic(), sendResult);
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
        System.out.printf("生产端:syncSend topic %s tag %s %n", mqPropertiesConfig.getMsgExtTopic(), "tag0");
        rocketMQTemplate.convertAndSend(mqPropertiesConfig.getMsgExtTopic() + ":tagA", "I'm from tagA");
        System.out.printf("生产端:syncSend topic %s tag %s %n", mqPropertiesConfig.getMsgExtTopic(), "tagA");
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
        System.out.printf("生产端:syncSend to topic %s sendResult=%s %n", userTopic, sendResult);
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
                System.out.printf("生产端:asyncSend to topic sendResult=%s %n", sendResult);
            }

            @Override
            public void onException(Throwable e) {
                e.printStackTrace();
                System.out.printf("生产端:asyncSend to topic fail,e=%s %n", e);
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
        System.out.printf("生产端:send %s and receive %s %n", "request string", replyString);
        return replyString;
    }


    /**
     * 顺序消息(分区类顺序消息)
     * @return
     */
    @RequestMapping("/syncSendOrderly")
    public String syncSendOrderly(String message) {
        String stringOrderlyTopic = mqPropertiesConfig.getStringOrderlyTopic();

        // Send request in sync mode and receive a reply of String type.

        for (int i = 1; i < 12; i++) {
            Message<String> message1 = MessageBuilder.withPayload(i + " " + message)
                    .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE).build();
            SendResult sendResult = rocketMQTemplate.syncSendOrderly(stringOrderlyTopic, message1, String.valueOf(i));
            System.out.printf("生产端:send %s and orderBy %s %n", "request string", message);
        }

        return "顺序消息发送完成";
    }


    /**
     * 发送字符串
     *
     * %RETRY%消费组名称 重投队列
     * %DLQ%消费组名称  死信队列
     * @param message
     * @return
     */
    @RequestMapping(value = "/sendtest")
    public String sendtest(String message,int size) {
        int count = 0;
        for (int i = 0; i < size; i++) {
            count++;
            // Send string
            SendResult sendResult = rocketMQTemplate.syncSend("test", i);
            System.out.printf("生产端:syncSend to topic %s sendResult=%s %n", mqPropertiesConfig.getSpringTopic(), sendResult);
        }

        System.out.println("生产端消息发送完成,总共发送消息数量count=" + count);

        return "生产端消息发送完成,总共发送消息数量count=" + count;
    }


}
