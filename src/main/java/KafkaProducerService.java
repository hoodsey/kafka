import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaProducerService(String bootstrapServers, String topic) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(producerProps);
        this.topic = topic;
    }

    public void sendMessage(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                logger.error("Error sending message with key '{}':", key, exception);
            } else {
                logger.info("Message sent successfully to topic '{}', partition {}, offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
        producer.flush();
    }

    public void sendDelayedMessage(String message, long delay) {
        new Thread(() -> {
            try {
                Thread.sleep(delay);
                sendMessage(null, message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    public void close() {
        producer.close();
        logger.info("Kafka producer closed.");
    }
}