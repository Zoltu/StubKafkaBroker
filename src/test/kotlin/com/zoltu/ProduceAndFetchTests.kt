package com.zoltu

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.Test
import java.time.Duration
import kotlin.test.assertEquals

class ProduceAndFetchTests {
	@Test
	fun `given a primed topic - on alternating produce and fetch - each fetch returns the previously produced record`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		stubKafkaBroker.addTopic("my topic")
		val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
		kafkaConsumer.assign(listOf(TopicPartition("my topic", 0)))

		/* act */
		kafkaProducer.send(ProducerRecord("my topic", "zip".toByteArray())).get()!!
		val consumerRecords1 = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic")
		kafkaProducer.send(ProducerRecord("my topic", "zap".toByteArray())).get()!!
		val consumerRecords2 = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic")
		kafkaProducer.send(ProducerRecord("my topic", "baz".toByteArray())).get()!!
		val consumerRecords3 = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic")

		/* assert */
		assertEquals("zip", consumerRecords1!!.first()!!.value()!!.toString(Charsets.UTF_8))
		assertEquals("zap", consumerRecords2!!.first()!!.value()!!.toString(Charsets.UTF_8))
		assertEquals("baz", consumerRecords3!!.first()!!.value()!!.toString(Charsets.UTF_8))
	}
}
