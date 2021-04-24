package cn.king.consumer;

import cn.king.entry.avro.generated.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author: wjl@king.cn
 * @time: 2021-04-24 18:50
 * @version: 1.0.0
 * @description:
 * @why:
 */
@Slf4j
public class UserConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "aliyun:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 注意此处value的序列化方式
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("group.id", "consumer_group_" + System.currentTimeMillis());
        properties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("user_topic_test"));

        // 注意此处传递了User.getClassSchema()，意味着该项目中User的全类名必须是User.getClassSchema()指定的全类名
        SpecificDatumReader<User> reader = new SpecificDatumReader<>(User.getClassSchema());
        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(10);
                for (ConsumerRecord<String, byte[]> record : records) {
                    Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
                    User user;
                    try {
                        user = reader.read(null, decoder);
                        System.out.println(record.key() + ":" + user.get("id") + "\t" + user.get("username") + "\t" + user.get("age"));
                    } catch (Exception e) {
                        log.error("", e);
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}
