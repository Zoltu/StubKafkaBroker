package com.zoltu

import com.zoltu.extensions.copyToArray
import com.zoltu.extensions.groupBy
import com.zoltu.extensions.toByteBuffer
import com.zoltu.extensions.toScalaImmutableMap
import com.zoltu.extensions.toScalaMap
import com.zoltu.extensions.toScalaSeq
import kafka.api.FetchResponse
import kafka.api.FetchResponsePartitionData
import kafka.api.GroupCoordinatorResponse
import kafka.api.OffsetFetchResponse
import kafka.api.PartitionMetadata
import kafka.api.ProducerResponse
import kafka.api.ProducerResponseStatus
import kafka.api.TopicMetadata
import kafka.api.TopicMetadataResponse
import kafka.cluster.BrokerEndPoint
import kafka.common.ErrorMapping
import kafka.common.OffsetMetadata
import kafka.common.OffsetMetadataAndError
import kafka.common.TopicAndPartition
import kafka.message.ByteBufferMessageSet
import kafka.message.Message
import kafka.message.`NoCompressionCodec$`
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.ControlledShutdownRequest
import org.apache.kafka.common.requests.DescribeGroupsRequest
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.GroupCoordinatorRequest
import org.apache.kafka.common.requests.HeartbeatRequest
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.LeaderAndIsrRequest
import org.apache.kafka.common.requests.LeaveGroupRequest
import org.apache.kafka.common.requests.ListGroupsRequest
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.OffsetFetchRequest
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.StopReplicaRequest
import org.apache.kafka.common.requests.SyncGroupRequest
import org.apache.kafka.common.requests.UpdateMetadataRequest
import scala.Option
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

class StubKafkaBroker {
	/**
	 * The BrokerEndPoint of this kafka broker.  Contains useful information like its hostname and port.
	 */
	val thisBroker: BrokerEndPoint get() = BrokerEndPoint(0, lengthPrefixedMessageServer.host, lengthPrefixedMessageServer.port)

	/**
	 * The handler for metadata requests.
	 *
	 * Default behavior: automatically prime any topics specified in the request with this broker as the leader; respond with a all of the primed brokers/topics
	 */
	var metadataRequestHandler: (RequestHeader, MetadataRequest) -> TopicMetadataResponse = { requestHeader, metadataRequest ->
		metadataRequest.topics().forEach { addTopic(Topic.createSimple(it, thisBroker)) }
		TopicMetadataResponse(brokers.toScalaSeq(), topics.map { it.toTopicMetadata() }.toScalaSeq(), requestHeader.correlationId())
	}

	/**
	 * The handler for produce requests.
	 *
	 * Default Behavior: track produced messages; ack a successful write to the supplied topic/partition with an offset of 0.
	 */
	var produceRequestHandler: (RequestHeader, ProduceRequest) -> ProducerResponse = { requestHeader, produceRequest ->
		internalProducedMessages = internalProducedMessages.plus(ProducedMessage.create(produceRequest))
		val responses = produceRequest
				.partitionRecords()!!
				.keys
				.filterNotNull()
				.map { TopicAndPartition(it.topic(), it.partition()) }
				.associateBy({ it }, { ProducerResponseStatus(0, 0) })
		ProducerResponse(requestHeader.correlationId(), responses.toScalaMap(), requestHeader.apiVersion().toInt(), 0)
	}

	/**
	 * The handler for fetch requests.
	 *
	 * Default Behavior: respond with all messages published to the requested topics/partitions, skipping up to the supplied offset
	 */
	var fetchRequestHandler: (RequestHeader, FetchRequest) -> FetchResponse = { requestHeader, fetchRequest ->
		fun getTopicAndPartitionToPartitionDataMap(topic: String, partition: Int, startOffset: Long): Pair<TopicAndPartition, FetchResponsePartitionData> {
			val topicAndPartition = TopicAndPartition(topic, partition)
			val messages = producedMessagesByTopicAndPartition
					.getOrElse(topic, { emptyMap() })
					.getOrElse(partition, { emptyList() })
					.drop(startOffset.toInt())
					.filterNotNull()
					.map { Message(it) }
			val messageSet = ByteBufferMessageSet(`NoCompressionCodec$`.`MODULE$`, AtomicLong(startOffset.toLong()), messages.toScalaSeq())
			val partitionMetadata = FetchResponsePartitionData(ErrorMapping.NoError(), messages.size.toLong() - 1, messageSet)
			return Pair(topicAndPartition, partitionMetadata)
		}

		val responses = fetchRequest.fetchData().entries
				.asSequence()
				.filterNotNull()
				.filter { it.key != null }
				.filter { it.key!!.topic() != null }
				.filter { it.value != null }
				.associate { getTopicAndPartitionToPartitionDataMap(it.key!!.topic()!!, it.key!!.partition(), it.value!!.offset) }

		FetchResponse(requestHeader.correlationId(), responses.toScalaMap(), requestHeader.apiVersion().toInt(), 0)
	}

	val groupCoordinatorRequestHandler: (RequestHeader, GroupCoordinatorRequest) -> GroupCoordinatorResponse = { requestHeader, groupCoordinatorRequest ->
		GroupCoordinatorResponse(Option.apply(thisBroker), 0, requestHeader.correlationId())
	}

	val offsetFetchRequestHandler: (RequestHeader, OffsetFetchRequest) -> OffsetFetchResponse = { requestHeader, offsetFetchRequest ->
		val topicPartitionToMetadataMap = offsetFetchRequest.partitions().associateBy({ TopicAndPartition(it.topic(), it.partition()) }, { OffsetMetadataAndError(OffsetMetadata(0, ""), 0) })
		OffsetFetchResponse(topicPartitionToMetadataMap.toScalaImmutableMap(), requestHeader.correlationId())
	}

	private val lengthPrefixedMessageServer = LengthPrefixedMessageServer { processRequest(it) }
	private var brokers: Set<BrokerEndPoint> = setOf(thisBroker)
	private var topics: Set<Topic> = emptySet()
	private var internalProducedMessages = emptySequence<ProducedMessage>()

	/**
	 * All of the messages that have been produced by this broker (without topic/partition data)
	 */
	val producedMessages: Iterable<ByteArray?> get() = internalProducedMessages.map { it.message }.asIterable()

	/**
	 * All of the messages that have been produced to this broker, mapped by topics
	 */
	val producedMessagesByTopic: Map<String, List<ByteArray?>> get() = internalProducedMessages.groupBy({ it.topic }, { it.message })

	/**
	 * All of the messages that have been produced to this broker, mapped by topic and partition.
	 */
	val producedMessagesByTopicAndPartition: Map<String, Map<Int, List<ByteArray?>>> get() = internalProducedMessages.groupBy { it.topic }.mapValues { it.value.groupBy({ it.partition }, { it.message }) }

	/**
	 * Resets this broker to its default state.
	 */
	fun reset() {
		clearBrokers()
		clearTopics()
		clearMessages()
	}

	/**
	 * Adds a broker to the list of brokers this broker knows about.  Returned as part of metadata requests.
	 */
	fun addBroker(broker: BrokerEndPoint) {
		brokers += broker
	}

	/**
	 * Resets this broker to its default state where it only knows about itself.
	 */
	fun clearBrokers() {
		brokers = setOf(thisBroker)
	}

	/**
	 * Adds a topic to this broker.  Returned as part of metadata requests.
	 */
	fun addTopic(topic: Topic) {
		topics += topic
	}

	/**
	 * Clears all topics from this broker.
	 */
	fun clearTopics() {
		topics = emptySet()
	}

	/**
	 * Clears all messages produced to this broker.
	 */
	fun clearMessages() {
		internalProducedMessages = emptySequence<ProducedMessage>()
	}

	private fun processRequest(byteBuffer: ByteBuffer): ByteBuffer {
		val requestHeader = RequestHeader.parse(byteBuffer)
		val requestBody = AbstractRequest.getRequest(requestHeader.apiKey().toInt(), requestHeader.apiVersion().toInt(), byteBuffer)
		when (requestBody ) {
			is MetadataRequest -> return metadataRequestHandler(requestHeader, requestBody).toByteBuffer()
			is ProduceRequest -> return produceRequestHandler(requestHeader, requestBody).toByteBuffer()
			is FetchRequest -> return fetchRequestHandler(requestHeader, requestBody).toByteBuffer()
			is GroupCoordinatorRequest -> return groupCoordinatorRequestHandler(requestHeader, requestBody).toByteBuffer()
			is OffsetFetchRequest -> return offsetFetchRequestHandler(requestHeader, requestBody).toByteBuffer()
			is ListOffsetRequest, is LeaderAndIsrRequest, is StopReplicaRequest, is ControlledShutdownRequest, is UpdateMetadataRequest, is OffsetCommitRequest, is JoinGroupRequest, is HeartbeatRequest, is LeaveGroupRequest, is SyncGroupRequest, is DescribeGroupsRequest, is ListGroupsRequest -> {
				throw UnsupportedOperationException("Unhandled request type.")
			}
			else -> throw UnsupportedOperationException("Unhandled request type.")
		}
	}

	data class Topic(val topicName: String, val partitions: Array<Partition>) {
		fun toTopicMetadata(): TopicMetadata = TopicMetadata(topicName, partitions.map { it.toPartitionMetadata() }.toScalaSeq(), 0)

		companion object Factory {
			fun createSimple(topicName: String, partitionLeader: BrokerEndPoint) = Topic(topicName, arrayOf(Partition.createSimple(0, partitionLeader)))
		}
	}

	data class Partition(val id: Int, val leader: BrokerEndPoint?, val replicas: Array<BrokerEndPoint>, val inSyncReplicas: Array<BrokerEndPoint>) {
		fun toPartitionMetadata(): PartitionMetadata = PartitionMetadata(id, Option.apply(leader), replicas.toScalaSeq(), inSyncReplicas.toScalaSeq(), 0)

		companion object Factory {
			fun createSimple(id: Int, leader: BrokerEndPoint) = Partition(id, leader, emptyArray(), emptyArray())
		}
	}

	internal data class ProducedMessage(val topic: String, val partition: Int, val key: ByteArray?, val message: ByteArray?) {
		companion object Factory {
			internal fun create(produceRequest: ProduceRequest): Sequence<ProducedMessage> {
				return (produceRequest.partitionRecords() ?: emptyMap<TopicPartition?, ByteBuffer?>()).asSequence()
						.mapNotNull map@ { topicAndPartitionToByteBufferMapEntry ->
							val topicAndPartition = topicAndPartitionToByteBufferMapEntry.key ?: return@map null
							val byteBuffer = topicAndPartitionToByteBufferMapEntry.value ?: return@map null
							val topic = topicAndPartition.topic() ?: return@map null
							object {
								val topic = topic
								val partition = topicAndPartition.partition()
								val records = MemoryRecords.readableRecords(byteBuffer).asSequence().map { logEntry -> logEntry.record() }.filterNotNull()
							}
						}
						.flatMap { topicPartitionRecords ->
							topicPartitionRecords.records.mapNotNull map@ { record ->
								val key = record.key()?.copyToArray()
								val value = record.value()?.copyToArray()
								ProducedMessage(topicPartitionRecords.topic, topicPartitionRecords.partition, key, value)
							}
						}
			}
		}
	}
}
