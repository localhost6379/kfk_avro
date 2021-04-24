package cn.king.properties;


import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * @author: wjl@king.cn
 * @time: 2021-04-24 18:15
 * @version: 1.0.0
 * @description:
 * @why:
 */
@Data
@Component
@PropertySource(value = {"classpath:application.yml"}, encoding = "UTF-8")
@ConfigurationProperties(prefix = "input")
public class KfkProperties {

    @Value("${input.path}")
    private String path;

    @Value("${input.topic}")
    private String topic;

    @Value("${input.regex}")
    private String regex;

    @Value("${input.bootstrapServers}")
    private String bootstrapServers;

}
