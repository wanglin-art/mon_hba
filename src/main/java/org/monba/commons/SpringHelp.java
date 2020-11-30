package org.monba.commons;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringHelp {

    // 初始化Spring容器，加载配置文件
    private static final ApplicationContext ac = new ClassPathXmlApplicationContext("applicationContext.xml");

    public static ApplicationContext getApplicationContext() {
        return ac;
    }
}
