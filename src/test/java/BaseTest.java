import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
@Testcontainers
abstract public class BaseTest {

    @Container
    protected static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.0"));
    protected KafkaConsumerService kafkaConsumerService;
    protected KafkaProducerService kafkaProducerService;
    private String topic = "test-topic";

    @BeforeEach
    void setUp() {
        kafkaContainer.start();
        kafkaConsumerService = new KafkaConsumerService(kafkaContainer.getBootstrapServers(), topic, 1000);
        kafkaProducerService = new KafkaProducerService(kafkaContainer.getBootstrapServers(), topic);
    }

    @AfterEach
    void tearDown() {
        kafkaProducerService.close();
        kafkaConsumerService.close();
    }
}
