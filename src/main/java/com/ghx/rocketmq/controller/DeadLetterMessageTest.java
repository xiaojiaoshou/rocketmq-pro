package com.ghx.rocketmq.controller;

import com.ghx.rocketmq.config.MqPropertiesConfig;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @description:
 * @type: JAVA
 * @since: 2020/11/29 17:53
 * @author: guohuixiang
 */
@RequestMapping("/test")
@RestController
public class DeadLetterMessageTest {

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
    @RequestMapping(value = "/deadLetter")
    public SendResult sendString(String message) {
        // Send string
        SendResult sendResult = rocketMQTemplate.syncSend("deadLetter", message);
        System.out.printf("syncSend1 to topic %s sendResult=%s %n", "deadLetter", sendResult);
        return sendResult;
    }

}
