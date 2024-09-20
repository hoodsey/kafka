import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final int pollTimeout;

    public KafkaConsumerService(String bootstrapServers, String topic, int pollTimeout) {
        this.topic = topic;
        this.pollTimeout = pollTimeout;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        logger.info("KafkaConsumerService  инициализация для топика: {}", topic);
    }

    public void consumeMessages(MessageProcessor processor, long waitTime, CountDownLatch latch) {
        try {
            consumer.subscribe(Collections.singletonList(topic));

            long endTime = System.currentTimeMillis() + waitTime;
            while (System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    processor.process(record);
                }
                if (latch.getCount() == 0) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public String consumeSingleMessage() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
        if (!records.isEmpty()) {
            String value = records.iterator().next().value();
            logger.info("Сообщение: {}", value);
            return value;
        }
        logger.info("Сообщения не потребляются; записи пусты");
        return null;
    }

    public void consumeMultipleMessages(int count, CountDownLatch latch) {
        new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
                    if (!records.isEmpty()) {
                        int consumedRecords = 0;
                        for (ConsumerRecord<String, String> record : records) {
                            logger.info("Сообщение: {}", record.value());
                            ++consumedRecords;
                        }
                        if (consumedRecords >= count) {
                            break;
                        }
                    } else {
                        logger.info("Сообщения не потребляются; записи пусты");
                    }
                }
            } catch (Exception e) {
                logger.error("Ошибка при потреблении сообщений: ", e);
            } finally {
                latch.countDown();
            }
        }).start();
    }

    public void consumeWithProcessor(KafkaConsumerService.MessageProcessor processor, CountDownLatch latch) {
        Thread consumerThread = new Thread(() -> {
            try {
                consumeMessages(processor, 5000, latch);
            } finally {
                latch.countDown();
            }
        });
        consumerThread.start();
        try {
            latch.await(); // Ожидание завершения
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Освобождаем ресурсы
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            consumer.close();
            logger.info("KafkaConsumer закрытие для топика: {}", topic);
        } catch (Exception e) {
            logger.error("Ошибка при закрытии KafkaConsumerService", e);
        }
    }

    public interface MessageProcessor {
        void process(ConsumerRecord<String, String> record);
    }
}