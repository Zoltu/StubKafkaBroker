package com.zoltu

import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Test
import java.time.Duration
import kotlin.test.assertEquals

class GroupManagementTests {
	@Test
	fun `given no primes - on commit - it yields no records`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()

		/* act */
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
		kafkaConsumer.subscribe(listOf("my topic"))
		val consumerRecords = kafkaConsumer.poll(0)

		/* assert */
		assert(consumerRecords.isEmpty)
	}

	@Test
	fun `given primed topic and messages produced to it - on subscribe and poll - it returns all produced records`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		stubKafkaBroker.addTopic("my topic")
		val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
		kafkaProducer.send(ProducerRecord("my topic", "zip".toByteArray())).get()!!
		kafkaProducer.send(ProducerRecord("my topic", "zap".toByteArray())).get()!!
		kafkaProducer.send(ProducerRecord("my topic", "baz".toByteArray())).get()!!

		/* act */
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
		kafkaConsumer.subscribe(listOf("my topic"))
		val consumerRecords = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic").toList()

		/* assert */
		assertEquals(3, consumerRecords.size)
		assertEquals("zip", consumerRecords[0]!!.value()!!.toString(Charsets.UTF_8))
		assertEquals("zap", consumerRecords[1]!!.value()!!.toString(Charsets.UTF_8))
		assertEquals("baz", consumerRecords[2]!!.value()!!.toString(Charsets.UTF_8))
	}
}
