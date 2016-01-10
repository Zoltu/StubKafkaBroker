package com.zoltu

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import java.util.Properties
import kotlin.test.assertEquals

class StubKafkaServerTests : org.jetbrains.spek.api.Spek() {
	init {
		given ("a stub kafka server with no priming") {
			val stubKafkaServer = StubKafkaServer()

			on ("connect and list topics") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaServer.thisBroker.port())
				val topics = kafkaConsumer.listTopics()

				it ("should return no topics") {
					assert(topics.size == 0)
				}
			}

			on("connect and list topics twice") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaServer.thisBroker.port())
				val topics1 = kafkaConsumer.listTopics()
				val topics2 = kafkaConsumer.listTopics()

				it("should return no topics on either call") {
					assert(topics1.size == 0)
					assert(topics2.size == 0)
				}
			}
		}

		given("a stub kafka server primed with one topic") {
			val stubKafkaServer = StubKafkaServer()
			stubKafkaServer.addTopic(StubKafkaServer.Topic.createSimple("my topic", stubKafkaServer.thisBroker))

			on("list topics") {
				//val kafkaConsumer = getDefaultKafkaConsumer(9092)
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaServer.thisBroker.port())
				val topics = kafkaConsumer.listTopics()

				it("should get the topic") {
					assertEquals("my topic", topics.entries.single().key)
				}
			}
		}

		given("a stub kafka server primed with one topic") {
			val stubKafkaServer = StubKafkaServer()
			stubKafkaServer.addTopic(StubKafkaServer.Topic.createSimple("my topic", stubKafkaServer.thisBroker))

			on("reset") {
				stubKafkaServer.reset()

				it("forgets the topic") {
					val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaServer.thisBroker.port())
					val topics = kafkaConsumer.listTopics()

					assert(topics.size == 0)
				}
			}
		}

		given("a stub kafka server with a down broker primed and a topic primed with the down broker as primary") {
			val stubKafkaServer = StubKafkaServer()
			val downBroker = BrokerEndPoint(1, "somewhere", 1234)
			stubKafkaServer.addBroker(downBroker)
			stubKafkaServer.addTopic(StubKafkaServer.Topic("my topic", arrayOf(StubKafkaServer.Partition(1, downBroker, emptyArray(), emptyArray()))))

			on("list topics multiple times") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaServer.thisBroker.port())
				val topics = kafkaConsumer.listTopics()

				it("should get the topic with the down broker as leader") {
					assertEquals(1, topics.size)
					val topic = topics.entries.single()
					assertEquals("my topic", topic.key)
					assertEquals(1, topic.value.size)
					val partition = topic.value.single()
					val leader = partition.leader()
					assertEquals(1, leader.id())
					assertEquals("somewhere", leader.host())
					assertEquals(1234, leader.port())
				}
			}
		}
	}

	fun getDefaultProperties(port: Int): Properties {
		val properties = Properties()
		properties.put("bootstrap.servers", "localhost:$port")
		properties.put("heartbeat.interval.ms", 100)
		properties.put("fetch.max.wait.ms", 500)
		properties.put("session.timeout.ms", 500)
		properties.put("request.timeout.ms", 1000)
		return properties
	}

	fun getDefaultKafkaConsumer(port: Int): KafkaConsumer<ByteArray, ByteArray> {
		val properties = getDefaultProperties(port)
		val keyDeserializer = ByteArrayDeserializer()
		val messageDeserializer = ByteArrayDeserializer()
		return KafkaConsumer<ByteArray, ByteArray>(properties, keyDeserializer, messageDeserializer)
	}
}
