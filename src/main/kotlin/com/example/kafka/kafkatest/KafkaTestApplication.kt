package com.example.kafka.kafkatest

import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.stereotype.Service
import org.springframework.util.backoff.FixedBackOff
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController


@SpringBootApplication
class KafkaTestApplication

fun main(args: Array<String>) {
    runApplication<KafkaTestApplication>(*args)
}

data class User(val name: String, val age: Int)

@Configuration
class KafkaConfig {

    @Bean
    fun kafkaListenerContainerFactory(
        configurer: ConcurrentKafkaListenerContainerFactoryConfigurer,
        kafkaConsumerFactory: ConsumerFactory<Any?, Any?>?,
        template: KafkaTemplate<Any?, Any?>?
    ): ConcurrentKafkaListenerContainerFactory<*, *>? {
        val factory = ConcurrentKafkaListenerContainerFactory<Any, Any>()
        configurer.configure(factory, kafkaConsumerFactory)
        return factory.apply {
            setCommonErrorHandler(DefaultErrorHandler(DeadLetterPublishingRecoverer(template!!), FixedBackOff(3000L, 3) ))
        }
    }

    @Bean
    fun defaultObjectMapper() = ObjectMapper().apply {
        disable(FAIL_ON_MISSING_CREATOR_PROPERTIES)
        disable(FAIL_ON_UNKNOWN_PROPERTIES)
        registerModule(KotlinModule.Builder().build())
        registerModule(JavaTimeModule())
    }


    @Bean
    fun startupRunner(producer: Producer) = CommandLineRunner {
        producer.sendMessage(User("Georg", 25))
        producer.sendMessage(User("Robert", 66))
        producer.sendMessage(User("Nicole", 55))
    }
}

@Service
class Producer(private val template: KafkaTemplate<String, User>) {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    companion object {
        const val TOPIC = "users"
    }

    fun sendMessage(user: User) {
        logger.info(String.format("#### -> Producing message -> %s", user))
        this.template.send(TOPIC, user)
    }
}

@Service
class Consumer {

    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["users"], groupId = "group_id")
    fun consume(message: User) {
        logger.info("#### -> Consumed message -> $message")
    }

}

@RestController
class ProducerController(private val producer: Producer) {

    @PostMapping(value = ["/produce"])
    fun produce(@RequestBody user: User) {
        producer.sendMessage(user)
    }
}




