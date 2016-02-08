package com.zoltu

import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ProduceTests {
	@Test
	fun `given a primed topic - on produce one nul message - it returns a successful write to the topic with partition 0 and offset 0`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		stubKafkaBroker.addTopic("my topic")

		/* act */
		val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
		val recordMetadata = kafkaProducer.send(ProducerRecord("my topic", null)).get()!!

		/* assert */
		assertEquals("my topic", recordMetadata.topic())
		assertEquals(0, recordMetadata.partition())
		assertEquals(0, recordMetadata.offset())
	}

	@Test
	fun `given a primed topic - on produce one null message - it records the produced messages`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		stubKafkaBroker.addTopic("my topic")

		/* act */
		val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
		kafkaProducer.send(ProducerRecord("my topic", null)).get()!!

		/* assert */
		val producedMessages = stubKafkaBroker.getProducedMessages("my topic", 0)
		assertEquals(1, producedMessages.size)
		val producedMessage = producedMessages.first()
		assertNull(producedMessage.key)
		assertNull(producedMessage.message)
	}

	@Test
	fun `given a primed topic - on produce two string messages - it records both productions`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		stubKafkaBroker.addTopic("my topic")

		/* act */
		val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
		kafkaProducer.send(ProducerRecord("my topic", "foo".toByteArray()))
		kafkaProducer.send(ProducerRecord("my topic", "bar".toByteArray())).get()!!

		/* assert */
		val producedMessages = stubKafkaBroker.getProducedMessages("my topic", 0)
		assertEquals(2, producedMessages.size)
		assertEquals("foo", producedMessages.first().message?.toString(Charsets.UTF_8))
		assertEquals("bar", producedMessages.last().message?.toString(Charsets.UTF_8))
	}
}
