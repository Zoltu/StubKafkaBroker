package com.zoltu

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.jetbrains.spek.api.Spek
import java.time.Duration
import kotlin.test.assertEquals

class TopicMetadataTests : Spek() {
	init {
		given ("no priming") {
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

			on ("request metadata for specific topic") {
				val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())
				val topicMetadata = kafkaProducer.partitionsFor("my topic")!!

				it("should return the requested topic with one partition lead by this broker") {
					assertEquals(1, topicMetadata.size)
					val partitionInfo = topicMetadata.single()!!
					assertEquals("my topic", partitionInfo.topic())
					assertEquals(0, partitionInfo.partition())
					assertEquals(stubKafkaBroker.thisBroker.host(), partitionInfo.leader().host())
					assertEquals(stubKafkaBroker.thisBroker.port(), partitionInfo.leader().port())
				}
			}
		}

		given("primed with one topic") {
			val stubKafkaBroker = StubKafkaBroker()
			stubKafkaBroker.addTopic("my topic")

			on("list topics") {
				val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
				val topics = kafkaConsumer.listTopics()

				it("should get the topic") {
					assertEquals("my topic", topics.entries.single().key)
				}
			}
		}

		given("primed with one topic") {
			val stubKafkaBroker = StubKafkaBroker()
			stubKafkaBroker.addTopic("my topic")

			on("reset") {
				stubKafkaBroker.reset()

				it("forgets the topic") {
					val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())
					val topics = kafkaConsumer.listTopics()
					assertEquals(0, topics.size)
				}
			}
		}

		given("a down broker primed and a topic primed with the down broker as primary") {
			val stubKafkaBroker = StubKafkaBroker()
			val downBroker = Node(1, "somewhere", 1234)
			stubKafkaBroker.addBroker(downBroker)
			stubKafkaBroker.addPartition(PartitionInfo("my topic", 1, downBroker, emptyArray(), emptyArray()))

			on("list topics") {
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

		given("a delayed response to MetadataRequest and a stub broker with a fast response") {
			val stubKafkaBrokerSlow = StubKafkaBroker()
			val stubKafkaBrokerFast = StubKafkaBroker()
			stubKafkaBrokerSlow.addTopic("my topic")
			stubKafkaBrokerFast.addTopic("my topic")
			val defaultMetadataRequestHandler = stubKafkaBrokerSlow.metadataRequestHandler
			stubKafkaBrokerSlow.metadataRequestHandler = { requestHeader, metadataRequest ->
				Thread.sleep(Duration.ofMillis(1100).toMillis())
				defaultMetadataRequestHandler(requestHeader, metadataRequest)
			}

			on("list topic") {
				val properties = getDefaultConsumerProperties(0)
				properties.put("bootstrap.servers", "localhost:${stubKafkaBrokerSlow.thisBroker.port()};localhost:${stubKafkaBrokerFast.thisBroker.port()}")
				val kafkaConsumer = KafkaConsumer<ByteArray, ByteArray>(properties, ByteArrayDeserializer(), ByteArrayDeserializer())
				val topics = kafkaConsumer.listTopics()

				it("should timeout") {
					assertEquals("my topic", topics.entries.single().key)
				}
			}
		}
	}
}
