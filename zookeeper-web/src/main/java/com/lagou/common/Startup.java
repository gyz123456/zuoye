package com.lagou.common;

import com.lagou.util.Utils;
import org.I0Itec.zkclient.ZkClient;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @Description 启动时初始化：创建数据库连接池并监听
 */

@Component
public class Startup {

    @PostConstruct
    public void init() throws IOException, SQLException, ClassNotFoundException {
        ZkClient zkClient = new ZkClient("linux121:2181");
        String cfg = zkClient.readData("/webapp/dblinkcfg", true);
        Properties pro = new Properties();
        Utils.loadData(pro, cfg);
        // 创建数据库连接池
        Utils.createDbPool(pro);
        // 监听节点数据的变化
        Listener.monitor();

        // 使用连接池测试
        Connection conn = ConnectionManager.getConnection();
        PreparedStatement preparedStatement = conn.prepareStatement("select * from execution_jobs");
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            System.out.println(resultSet.getString("flow_id"));
            System.out.println(resultSet.getString("job_id"));
        }
    }
}
