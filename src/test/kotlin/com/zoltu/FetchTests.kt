package com.zoltu

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class FetchTests {
	@Test
	fun `given a primed topic - on fetch - it yields no records`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		stubKafkaBroker.addTopic("my topic")

		/* act */
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
		kafkaConsumer.assign(listOf(TopicPartition("my topic", 0)))
		val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100).toMillis()).records("my topic")

		/* assert */
		assertEquals(0, consumerRecords.count())
	}

	@Test
	fun `given a primed topic with produced messages - on fetch - it yields produced messages`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		stubKafkaBroker.addTopic("my topic")
		val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
		kafkaProducer.send(ProducerRecord("my topic", "foo".toByteArray(Charsets.UTF_8)))
		kafkaProducer.send(ProducerRecord("my topic", "bar".toByteArray(Charsets.UTF_8))).get()!!

		/* act */
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
		kafkaConsumer.assign(listOf(TopicPartition("my topic", 0)))
		val consumerRecords = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic")

		/* assert */
		assertNotNull(consumerRecords)
		assertEquals(2, consumerRecords.count())
		assertEquals("foo", consumerRecords.first().value().toString(Charsets.UTF_8))
		assertEquals("bar", consumerRecords.last().value().toString(Charsets.UTF_8))
	}
}
