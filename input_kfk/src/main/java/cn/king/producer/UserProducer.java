package cn.king.producer;

import cn.king.entry.avro.generated.User;
import cn.king.properties.KfkProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.util.Properties;

/**
 * @author: wjl@king.cn
 * @time: 2021-04-24 17:52
 * @version: 1.0.0
 * @description:
 * @why:
 */
@Slf4j
@Component
public class UserProducer {

    private final KfkProperties kfkProperties;

    public UserProducer(KfkProperties kfkProperties) {
        this.kfkProperties = kfkProperties;
    }

    public void inputData() {

        String bootstrapServers = kfkProperties.getBootstrapServers();
        String topic = kfkProperties.getTopic();
        String path = kfkProperties.getPath();
        String regex = kfkProperties.getRegex();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // 批次大小. 一次发送多少大小的数据. 16k
        properties.put("batch.size", 16384);
        // 等待时间. 1ms. 两个参数控制发送数据, 要么数据大于16k,要么1ms之后发送数据.
        properties.put("linger.ms", 1);
        // 发送缓存区RecordAccumulator内存大小. 32M.
        // 数据到了16k发送到RecordAccumulator, RecordAccumulator 中最多放32M数据.
        properties.put("buffer.memory", 33554432);

        // 抛出异常之前能阻塞两秒
        //properties.put("max.block.ms", 10 * 1000);

        properties.put(ProducerConfig.RETRIES_CONFIG, "2");
        properties.put("compression.type", "gzip");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 注意此处value的序列化方式
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        UserCsvFileReader reader = new UserCsvFileReader(path,regex);

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(properties);

        for (; ; ) {
            User record = reader.get();

            if (null != record) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                SpecificDatumWriter<User> writer = new SpecificDatumWriter<>(record.getSchema());
                try {
                    writer.write(record, encoder);
                    encoder.flush();
                    out.close();
                } catch (Exception e) {
                    log.error("", e);
                }

                producer.send(new ProducerRecord<>(topic, out.toByteArray()), (metadata, exception) -> {
                    if (null == exception) {
                        // 如果发送成功, 打印消息的分区编号和偏移量
                        System.out.println(metadata.partition() + "--" + metadata.offset());
                    } else {
                        // 如果发送失败, 打印堆栈信息
                        log.error(exception.getMessage());
                    }
                });

            }
        }

    }

}
