package com.lagou.test;

import com.lagou.common.Publisher;
import org.junit.Test;

/**
 * @Description 测试发布信息
 */
public class PublisherTest {

    Publisher publisher = new Publisher();

    @Test
    public void publish() {
        String cfg = "driverClassName=com.mysql.jdbc.Driver\n" +
                "url=jdbc:mysql://linux121:3306/azkaban\n" +
                "username=root\n" +
                "password=123456\n" +
                "initCount=5\n" +
                "maxCount=10\n" +
                "currentCount=5";
        publisher.publish(cfg);
        System.out.println(publisher.zkClient.readData("/webapp/dblinkcfg", true).toString());
    }
}
