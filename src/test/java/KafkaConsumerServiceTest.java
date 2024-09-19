import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class KafkaConsumerServiceTest extends BaseTest {

    @Test
    @DisplayName("Проверка чтения без сообщений")
    void testConsumeFromEmpty() {
        String consumedMessage = kafkaConsumerService.consumeSingleMessage();
        Assertions.assertNull(consumedMessage, "Ожидается, что сообщения не будут получены");
    }

    @Test
    @DisplayName("Проверка, что сообщение, отправленное продюсером, может быть правильно прочитано")
    void testSendAndConsumeMessage() {
        String expectedMessage = "Hello, Kafka!";
        kafkaProducerService.sendMessage(null, expectedMessage);
        String consumedMessage = kafkaConsumerService.consumeSingleMessage();
        assertEquals(expectedMessage, consumedMessage);
    }

    @Test
    @DisplayName("Проверяем, что процессор корректно обрабатывает сообщение, используя CountDownLatch для синхронизации")
    void testConsumeWithProcessor() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        String expectedMessage = "processedTestValue";
        KafkaConsumerService.MessageProcessor processor = record -> {
            assertEquals(expectedMessage, record.value());
            latch.countDown();
        };
        kafkaProducerService.sendMessage(null, expectedMessage);
        kafkaConsumerService.consumeWithProcessor(processor, latch);
        boolean processed = latch.await(5, TimeUnit.SECONDS);
        assertTrue(processed, "Сообщение не было обработано за отведенное время");
    }

    @Test
    @DisplayName("Проверка обработки нескольких сообщений без ключей")
    void testConsumeMultiplesMessages() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        String[] messages = {"msg1", "msg2", "msg3"};
        for (String message : messages) {
            kafkaProducerService.sendMessage(null, message);
        }
        kafkaConsumerService.consumeMultipleMessages(messages.length, latch);
        boolean processed = latch.await(5, TimeUnit.SECONDS);
        assertTrue(processed, "Сообщения не были обработаны за отведенное время");
    }

    @Test
    @DisplayName("Проверка отправки на несуществующую тему")
    void testSubscribeToNonExistentTopic() {
        String nonExistentTopic = "non-existent-topic";
        KafkaConsumerService consumerService = new KafkaConsumerService(kafkaContainer.getBootstrapServers(), nonExistentTopic, 1000);
        String consumedMessage = consumerService.consumeSingleMessage();
        Assertions.assertNull(consumedMessage, "Expected no Ожидалось, что сообщения из несуществующей темы не будут использованы");
    }

    @Test
    @DisplayName("Проверка отложенного сообщения")
    void testConsumerDelayBehavior() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        String expectedMessage = "delayedMessage";
        kafkaProducerService.sendDelayedMessage(expectedMessage, 3000);
        Thread consumerThread = new Thread(() -> kafkaConsumerService.consumeMessages(record -> {
            assertEquals(expectedMessage, record.value());
            latch.countDown();
        }, 5000, latch));
        consumerThread.start();
        boolean processed = latch.await(10, TimeUnit.SECONDS);
        assertTrue(processed, "Отложенное сообщение не было обработано за отведенное время");
    }
}