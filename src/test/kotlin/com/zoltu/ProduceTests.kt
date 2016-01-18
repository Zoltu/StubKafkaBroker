package com.zoltu

import org.apache.kafka.clients.producer.ProducerRecord
import org.jetbrains.spek.api.Spek
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ProduceTests : Spek() {
	init {
		given("no priming") {
			val stubKafkaBroker = StubKafkaBroker()
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
	}
}
