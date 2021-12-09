package com.example.kafka.kafkatest

import io.kotest.matchers.collections.shouldContain
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import java.util.*

@SpringBootTest
@DirtiesContext
//we do not use the default port 9092 so that it does not collide with a local kafka instance
@EmbeddedKafka(partitions = 1, brokerProperties = ["listeners=PLAINTEXT://127.0.0.1:9988"])
@ActiveProfiles("test-kafka")
class EmbeddedKafkaIntegrationTest(@Autowired val producer: Producer) {

    companion object {
        val TEST_USER = User("Testname", 99)
    }

    val consumedUsers: MutableList<User> = Collections.synchronizedList(mutableListOf<User>())

    @Test
    fun produceLeadsToLocalConsume() {
        producer.sendMessage(TEST_USER)

        waitUntil { consumedUsers.contains(TEST_USER) }
        consumedUsers shouldContain TEST_USER
    }

    @KafkaListener(topics = ["users"], groupId = "EmbeddedKafkaIntegrationTest")
    fun consume(user: User) {
        consumedUsers += user
    }

}
