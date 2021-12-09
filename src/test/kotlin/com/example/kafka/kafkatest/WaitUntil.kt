package com.example.kafka.kafkatest

import java.time.LocalDateTime.now
import java.util.concurrent.TimeoutException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

@Throws(TimeoutException::class)
fun waitUntil(
    reason: () -> String? = { null },
    waitDuration: Duration = 10.seconds,
    condition: () -> Boolean) {

    val start = now()

    while (!condition()) {
        if(start + waitDuration.toJavaDuration() > now() ) {
            throw TimeoutException("Condition not meet within $waitDuration. ${reason() ?: "" }}")
        }
        Thread.sleep(500)
    }

}