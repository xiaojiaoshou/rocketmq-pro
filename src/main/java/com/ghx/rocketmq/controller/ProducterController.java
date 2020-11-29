package com.ghx.rocketmq.controller;

import com.ghx.rocketmq.config.MqPropertiesConfig;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
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


    @RequestMapping(value = "/syncSend-string")
    public SendResult sendString(String message) {
        // Send string
        SendResult sendResult = rocketMQTemplate.syncSend(mqPropertiesConfig.getSpringTopic(), message);
        System.out.printf("syncSend1 to topic %s sendResult=%s %n", mqPropertiesConfig.getSpringTopic(), sendResult);
        return sendResult;
    }



}
