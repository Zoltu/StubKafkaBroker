package com.zoltu

import kafka.cluster.BrokerEndPoint
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.jetbrains.spek.api.Spek
import java.time.Duration
import java.util.Properties
import kotlin.test.assertEquals

class TopicMetadataTests : Spek() {
	init {
		given ("a stub kafka server with no priming") {
			val stubKafkaBroker = StubKafkaBroker()

			on ("connect and list topics") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
				val topics = kafkaConsumer.listTopics()

				it ("should return no topics") {
					assert(topics.size == 0)
				}
			}

			on("connect and list topics twice") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
				val topics1 = kafkaConsumer.listTopics()
				val topics2 = kafkaConsumer.listTopics()

				it("should return no topics on either call") {
					assert(topics1.size == 0)
					assert(topics2.size == 0)
				}
			}
		}

		given("a stub kafka server primed with one topic") {
			val stubKafkaBroker = StubKafkaBroker()
			stubKafkaBroker.addTopic(StubKafkaBroker.Topic.createSimple("my topic", stubKafkaBroker.thisBroker))

			on("list topics") {
				//val kafkaConsumer = getDefaultKafkaConsumer(9092)
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
				val topics = kafkaConsumer.listTopics()

				it("should get the topic") {
					assertEquals("my topic", topics.entries.single().key)
				}
			}
		}

		given("a stub kafka server primed with one topic") {
			val stubKafkaBroker = StubKafkaBroker()
			stubKafkaBroker.addTopic(StubKafkaBroker.Topic.createSimple("my topic", stubKafkaBroker.thisBroker))

			on("reset") {
				stubKafkaBroker.reset()

				it("forgets the topic") {
					val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
					val topics = kafkaConsumer.listTopics()

					assert(topics.size == 0)
				}
			}
		}

		given("a stub kafka server with a down broker primed and a topic primed with the down broker as primary") {
			val stubKafkaBroker = StubKafkaBroker()
			val downBroker = BrokerEndPoint(1, "somewhere", 1234)
			stubKafkaBroker.addBroker(downBroker)
			stubKafkaBroker.addTopic(StubKafkaBroker.Topic("my topic", arrayOf(StubKafkaBroker.Partition(1, downBroker, emptyArray(), emptyArray()))))

			on("list topics multiple times") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
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

		given("a stub kafka broker with a delayed response to MetadataRequest and a stub broker with a fast response") {
			val stubKafkaBrokerSlow = StubKafkaBroker()
			val stubKafkaBrokerFast = StubKafkaBroker()
			stubKafkaBrokerSlow.addTopic(StubKafkaBroker.Topic.createSimple("my topic", stubKafkaBrokerFast.thisBroker))
			stubKafkaBrokerFast.addTopic(StubKafkaBroker.Topic.createSimple("my topic", stubKafkaBrokerSlow.thisBroker))
			val defaultMetadataRequestHandler = stubKafkaBrokerSlow.metadataRequestHandler
			stubKafkaBrokerSlow.metadataRequestHandler = { requestHeader, metadataRequest ->
				Thread.sleep(Duration.ofMillis(1100).toMillis())
				defaultMetadataRequestHandler(requestHeader, metadataRequest)
			}

			on("list topic") {
				val properties = getDefaultProperties(0)
				properties.put("bootstrap.servers", "localhost:${stubKafkaBrokerSlow.thisBroker.port()};localhost:${stubKafkaBrokerFast.thisBroker.port()}")
				val kafkaConsumer = KafkaConsumer<ByteArray, ByteArray>(properties, ByteArrayDeserializer(), ByteArrayDeserializer())
				val topics = kafkaConsumer.listTopics()

				it("should timeout") {
					assertEquals("my topic", topics.entries.single().key)
				}
			}
		}
	}

	fun getDefaultProperties(port: Int): Properties {
		val properties = Properties()
		properties.put("bootstrap.servers", "localhost:$port")
		// we want our tests to fail fast
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