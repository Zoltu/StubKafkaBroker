package com.zoltu

import org.apache.kafka.clients.producer.ProducerRecord
import org.jetbrains.spek.api.Spek
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class GroupManagementTests : Spek() {
	init {
		given("no priming") {
			val stubKafkaBroker = StubKafkaBroker()

			on("commit") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
				kafkaConsumer.subscribe(listOf("my topic"))
				val consumerRecords = kafkaConsumer.poll(0)

				it("returned no records") {
					assert(consumerRecords.isEmpty)
				}
			}
		}

		given("primed with topic and messages produced") {
			val stubKafkaBroker = StubKafkaBroker()
			stubKafkaBroker.addTopic("my topic")
			val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
			kafkaProducer.send(ProducerRecord("my topic", "zip".toByteArray())).get()!!
			kafkaProducer.send(ProducerRecord("my topic", "zap".toByteArray())).get()!!
			kafkaProducer.send(ProducerRecord("my topic", "baz".toByteArray())).get()!!

			on("subscribe and poll") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
				kafkaConsumer.subscribe(listOf("my topic"))
				val consumerRecords = kafkaConsumer.poll(Duration.ofMinutes(1000).toMillis()).records("my topic").toList()

				it("returns all 3 records") {
					assertNotNull(consumerRecords)

					assertEquals(3, consumerRecords.size)
					assertEquals("zip", consumerRecords[0].value().toString(Charsets.UTF_8))
					assertEquals("zap", consumerRecords[1].value().toString(Charsets.UTF_8))
					assertEquals("baz", consumerRecords[2].value().toString(Charsets.UTF_8))
				}
			}
		}
	}
}
