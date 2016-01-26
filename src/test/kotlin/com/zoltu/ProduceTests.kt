package com.zoltu

import org.apache.kafka.clients.producer.ProducerRecord
import org.jetbrains.spek.api.Spek
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ProduceTests : Spek() {
	init {
		given("primed with topic") {
			val stubKafkaBroker = StubKafkaBroker()
			// we have to prime a topic or else the metadata requests will fail before any production is allowed by the client
			stubKafkaBroker.addTopic(StubKafkaBroker.Topic.createSimple("my topic", stubKafkaBroker.thisBroker))

			on("produce one null message") {
				val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
				val recordMetadata = kafkaProducer.send(ProducerRecord("my topic", null)).get()!!

				it("returns a successful write to the topic, partition 0 and offset 0") {
					assertEquals("my topic", recordMetadata.topic())
					assertEquals(0, recordMetadata.partition())
					assertEquals(0, recordMetadata.offset())
				}

				it("contains the produced message as part of its produces") {
					assertNull(stubKafkaBroker.producedMessages.first())
					assertNull(stubKafkaBroker.producedMessagesByTopic.get("my topic")!!.first())
					assertNull(stubKafkaBroker.producedMessagesByTopicAndPartition.get("my topic")!!.get(0)!!.first())
				}
			}
		}

		given("primed with topic") {
			val stubKafkaBroker = StubKafkaBroker()
			// we have to prime a topic or else the metadata requests will fail before any production is allowed by the client
			stubKafkaBroker.addTopic(StubKafkaBroker.Topic.createSimple("my topic", stubKafkaBroker.thisBroker))

			on("produce two string message") {
				val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
				kafkaProducer.send(ProducerRecord("my topic", "foo".toByteArray()))
				kafkaProducer.send(ProducerRecord("my topic", "bar".toByteArray())).get()!!

				it("contains the produced messages") {
					assertEquals("foo", stubKafkaBroker.producedMessagesByTopic["my topic"]?.first()?.toString(Charsets.UTF_8))
					assertEquals("bar", stubKafkaBroker.producedMessagesByTopic["my topic"]?.last()?.toString(Charsets.UTF_8))
				}
			}
		}
	}
}
