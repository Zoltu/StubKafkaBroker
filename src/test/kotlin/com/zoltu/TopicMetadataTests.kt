package com.zoltu

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.junit.Test
import java.time.Duration
import kotlin.test.assertEquals

class TopicMetadataTests {
	@Test
	fun `when not primed - on list topics - it should not return any topics`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())

		/* act */
		val topics = kafkaConsumer.listTopics()

		/* assert */
		assertEquals(0, topics.size)
	}

	@Test
	fun `when not primed - on list topics multiple times - it should not return any topics`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())

		/* act */
		val topics1 = kafkaConsumer.listTopics()
		val topics2 = kafkaConsumer.listTopics()

		/* assert */
		assertEquals(0, topics1.size)
		assertEquals(0, topics2.size)
	}

	@Test
	fun `when not primed - on request metadata for a specific topic - it should return the requested topic with one partition lead by this broker`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		val kafkaProducer = getDefaultKafkaProducer(stubKafkaBroker.thisBroker.port())

		/* act */
		val topicMetadata = kafkaProducer.partitionsFor("my topic")!!

		/* assert */
		assertEquals(1, topicMetadata.size)
		val partitionInfo = topicMetadata.single()!!
		assertEquals("my topic", partitionInfo.topic())
		assertEquals(0, partitionInfo.partition())
		assertEquals(stubKafkaBroker.thisBroker.host(), partitionInfo.leader().host())
		assertEquals(stubKafkaBroker.thisBroker.port(), partitionInfo.leader().port())
	}

	@Test
	fun `when primed with one topic - on list topics - it should return the primed topic`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		stubKafkaBroker.addTopic("my topic")
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())

		/* act */
		val topics = kafkaConsumer.listTopics()

		/* assert */
		assertEquals("my topic", topics.entries.single().key)
	}

	@Test
	fun `when primed with one topic - on reset then list topics - it should not return any topics`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		stubKafkaBroker.addTopic("my topic")
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())

		/* act */
		stubKafkaBroker.reset()
		val topics = kafkaConsumer.listTopics()

		/* assert */
		assertEquals(0, topics.size)
	}

	@Test
	fun `when primed with one topic lead by another broker - on list topics - it shoud get the topic with the other broker as leader`() {
		/* arrange */
		val stubKafkaBroker = StubKafkaBroker()
		val otherBroker = Node(1, "somewhere", 1234)
		stubKafkaBroker.addBroker(otherBroker)
		stubKafkaBroker.addPartition(PartitionInfo("my topic", 1, otherBroker, emptyArray(), emptyArray()))
		val kafkaConsumer = getDefaultKafkaConsumer(stubKafkaBroker.thisBroker.port())

		/* act */
		val topics = kafkaConsumer.listTopics()

		/* assert */
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

	@Test
	fun `when primed with a slow responding broker but a secondary broker is fast - on list topics - it should get the topics from the fast broker`() {
		/* arrange */
		val stubKafkaBrokerSlow = StubKafkaBroker()
		stubKafkaBrokerSlow.addTopic("my topic slow")
		val defaultMetadataRequestHandler = stubKafkaBrokerSlow.metadataRequestHandler
		stubKafkaBrokerSlow.metadataRequestHandler = { requestHeader, metadataRequest ->
			Thread.sleep(Duration.ofSeconds(2).toMillis())
			defaultMetadataRequestHandler(requestHeader, metadataRequest)
		}
		val stubKafkaBrokerFast = StubKafkaBroker()
		stubKafkaBrokerFast.addTopic("my topic fast")
		val consumerProperties = getDefaultConsumerProperties(0)
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:${stubKafkaBrokerSlow.thisBroker.port()};localhost:${stubKafkaBrokerFast.thisBroker.port()}")
		val kafkaConsumer = KafkaConsumer<ByteArray, ByteArray>(consumerProperties, ByteArrayDeserializer(), ByteArrayDeserializer())

		/* act */
		val topics = kafkaConsumer.listTopics()

		/* assert */
		assertEquals("my topic fast", topics.entries.single().key)
	}
}
