package streaming;

import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Controller
@EnableAutoConfiguration
public class RestController {

    final List<String> RESULT = new ArrayList<>();

    @RequestMapping("/")
    @ResponseBody
    String home() {
        Configuration config = Configuration.create()
                                    /* begin engine properties */
                .with("connector.class",
                        "io.debezium.connector.mongodb.MongoDbConnector")
                .with("offset.storage",
                        "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename",
                        "/home/nikita/IdeaProjects/test/offset.dat")
                .with("offset.flush.interval.ms", 60000)

                /* begin connector properties */
                .with("name", "test123")
                .with("mongodb.name", "test")
                .with("mongodb.hosts", "localhost:27017")
                .with("database.whitelist", "test6")
                .with("collection.whitelist", "test6.collect")

                .build();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Create the engine with this configuration ...
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(
                        (record) -> {
                            producer.send(
                                    new ProducerRecord<String, String>(
                                            record.topic(),
                                            record.key().toString(),
                                            record.value().toString()));
                        }
                )
                .build();

        // Run the engine asynchronously ...
        Executor executor = Executors.newFixedThreadPool(4);
        executor.execute(engine);

        return "Start";
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(RestController.class, args);
    }
}