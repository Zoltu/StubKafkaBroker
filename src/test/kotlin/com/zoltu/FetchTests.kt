package com.zoltu

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.jetbrains.spek.api.Spek
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class FetchTests : Spek() {
	init {
		given("primed with topic") {
			val stubKafkaBroker = StubKafkaBroker()
			stubKafkaBroker.addTopic(StubKafkaBroker.Topic.createSimple("my topic", stubKafkaBroker.thisBroker))

			on("fetch") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
				kafkaConsumer.assign(listOf(TopicPartition("my topic", 0)))
				val consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100).toMillis()).records("my topic")

				it("is not null") {
					assertNotNull(consumerRecords)
					assertEquals(0, consumerRecords.count())
				}
			}
		}

		given("primed with topic with messages") {
			val stubKafkaBroker = StubKafkaBroker()
			stubKafkaBroker.addTopic(StubKafkaBroker.Topic.createSimple("my topic", stubKafkaBroker.thisBroker))
			val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
			kafkaProducer.send(ProducerRecord("my topic", "foo".toByteArray(Charsets.UTF_8)))
			kafkaProducer.send(ProducerRecord("my topic", "bar".toByteArray(Charsets.UTF_8))).get()!!

			on("fetch") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
				kafkaConsumer.assign(listOf(TopicPartition("my topic", 0)))
				val consumerRecords = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic")

				it("is not null") {
					assertNotNull(consumerRecords)
					assertEquals(2, consumerRecords.count())
					assertEquals("foo", consumerRecords.first().value().toString(Charsets.UTF_8))
					assertEquals("bar", consumerRecords.last().value().toString(Charsets.UTF_8))
				}
			}
		}
	}
}
