package com.ghx.rocketmq.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 *
 * @description:
 * @type: JAVA
 * @since: 2020/11/29 9:57
 * @author: guohuixiang
 */
@Data
@Configuration
public class MqPropertiesConfig {


    @Value("${demo.rocketmq.transTopic}")
    private String springTransTopic;
    @Value("${demo.rocketmq.topic}")
    private String springTopic;
    @Value("${demo.rocketmq.topic.user}")
    private String userTopic;

    @Value("${demo.rocketmq.orderTopic}")
    private String orderPaidTopic;
    @Value("${demo.rocketmq.msgExtTopic}")
    private String msgExtTopic;
    @Value("${demo.rocketmq.stringRequestTopic}")
    private String stringRequestTopic;
    @Value("${demo.rocketmq.bytesRequestTopic}")
    private String bytesRequestTopic;
    @Value("${demo.rocketmq.objectRequestTopic}")
    private String objectRequestTopic;
    @Value("${demo.rocketmq.genericRequestTopic}")
    private String genericRequestTopic;

    @Value("${demo.rocketmq.stringOrderlyTopic}")
    private String stringOrderlyTopic;
}
