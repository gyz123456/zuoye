package com.lagou.common;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: gyz
 * @Date: 2021/11/3 22:29
 * https://blog.csdn.net/u012543380/article/details/110083988
 * 修改数据库配置信息接口
 */
public class Publisher {

    Logger logger = LoggerFactory.getLogger(Publisher.class);
    public ZkClient zkClient = null;
    public static final String serverstring = "linux121:2181";
    //
    public static final String path = "/webapp/dblinkcfg";

    /**
     * 获取zk连接对象
     */
    private void connectZk() {
        zkClient = new ZkClient(serverstring);
        if (!zkClient.exists(path)) {
            // 创建保存数据库连接信息的节点
            zkClient.createPersistent(path, true);
        }
    }

    /**
     * 发布数据库配置信息
     *
     * @param cfgInfo 连接信息
     */
    public void publish(String cfgInfo) {
        connectZk();
        zkClient.writeData(path, cfgInfo);
        logger.info("发布数据库连接信息成功：" + cfgInfo);
    }



}
