package com.zoltu

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.jetbrains.spek.api.Spek
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ProduceAndFetchTests : Spek() {
	init {
		given("primed with topic") {
			val stubKafkaBroker = StubKafkaBroker()
			stubKafkaBroker.addTopic(StubKafkaBroker.Topic.createSimple("my topic", stubKafkaBroker.thisBroker))

			on("alternating produce and fetch") {
				val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
				kafkaConsumer.assign(listOf(TopicPartition("my topic", 0)))

				kafkaProducer.send(ProducerRecord("my topic", "zip".toByteArray())).get()!!
				val consumerRecords1 = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic")
				kafkaProducer.send(ProducerRecord("my topic", "zap".toByteArray())).get()!!
				val consumerRecords2 = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic")
				kafkaProducer.send(ProducerRecord("my topic", "baz".toByteArray())).get()!!
				val consumerRecords3 = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic")

				it("returns one record with each iteration") {
					assertNotNull(consumerRecords1)
					assertNotNull(consumerRecords2)
					assertNotNull(consumerRecords3)

					assertEquals("zip", consumerRecords1.first().value().toString(Charsets.UTF_8))
					assertEquals("zap", consumerRecords2.first().value().toString(Charsets.UTF_8))
					assertEquals("baz", consumerRecords3.first().value().toString(Charsets.UTF_8))
				}
			}
		}
	}
}
