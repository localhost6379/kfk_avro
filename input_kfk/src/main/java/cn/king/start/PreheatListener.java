package cn.king.start;

import cn.king.producer.UserProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

/**
 * @author: wjl@king.cn
 * @time: 2021-04-24 18:22
 * @version: 1.0.0
 * @description:
 * @why:
 */
@Slf4j
@Component
public class PreheatListener implements ApplicationListener<ContextRefreshedEvent> {

    private final UserProducer producer;

    public PreheatListener(UserProducer producer) {
        this.producer = producer;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        try {
            producer.inputData();
        } catch (Exception e) {
            log.error("", e);
        }
    }

}
