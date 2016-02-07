package com.zoltu

import com.zoltu.extensions.copyToArray
import com.zoltu.extensions.toResponseByteBuffer
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.requests.ControlledShutdownRequest
import org.apache.kafka.common.requests.DescribeGroupsRequest
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.common.requests.GroupCoordinatorRequest
import org.apache.kafka.common.requests.GroupCoordinatorResponse
import org.apache.kafka.common.requests.HeartbeatRequest
import org.apache.kafka.common.requests.HeartbeatResponse
import org.apache.kafka.common.requests.JoinGroupRequest
import org.apache.kafka.common.requests.JoinGroupResponse
import org.apache.kafka.common.requests.LeaderAndIsrRequest
import org.apache.kafka.common.requests.LeaveGroupRequest
import org.apache.kafka.common.requests.LeaveGroupResponse
import org.apache.kafka.common.requests.ListGroupsRequest
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.requests.MetadataResponse
import org.apache.kafka.common.requests.OffsetCommitRequest
import org.apache.kafka.common.requests.OffsetCommitResponse
import org.apache.kafka.common.requests.OffsetFetchRequest
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.requests.ProduceResponse
import org.apache.kafka.common.requests.RequestHeader
import org.apache.kafka.common.requests.StopReplicaRequest
import org.apache.kafka.common.requests.SyncGroupRequest
import org.apache.kafka.common.requests.SyncGroupResponse
import org.apache.kafka.common.requests.UpdateMetadataRequest
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.CopyOnWriteArrayList

class StubKafkaBroker {
	/**
	 * The BrokerEndPoint of this kafka broker.  Contains useful information like its hostname and port.
	 */
	val thisBroker: Node get() = Node(0, lengthPrefixedMessageServer.host, lengthPrefixedMessageServer.port)

	/**
	 * The handler for metadata requests.
	 *
	 * Default behavior: automatically prime any topics specified in the request with this broker as the leader; respond with a all of the primed brokers/topics
	 */
	var metadataRequestHandler: (RequestHeader, MetadataRequest) -> MetadataResponse = { requestHeader, metadataRequest ->
		metadataRequest.topics().forEach { addTopic(it) }

		val partitions = partitions.values.flatMap { it.values }
		val cluster = Cluster(brokers, partitions, emptySet())
		MetadataResponse(cluster, emptyMap())
	}

	/**
	 * The handler for produce requests.
	 *
	 * Default Behavior: track produced messages; ack a successful write to the supplied topic/partition with an offset of 0.
	 */
	var produceRequestHandler: (RequestHeader, ProduceRequest) -> ProduceResponse = { requestHeader, produceRequest ->
		fun toPartitionResponse(topic: String, partition: Int, byteBuffer: ByteBuffer): ProduceResponse.PartitionResponse {
			val newRecords = MemoryRecords.readableRecords(byteBuffer)
					.asSequence()
					.map { logEntry -> logEntry.record() }
					.filterNotNull()
					.toList()

			val allRecords = internalProducedMessages
					.computeIfAbsent(topic, { ConcurrentHashMap() })
					.computeIfAbsent(partition, { CopyOnWriteArrayList() });

			// FIXME: race condition here! another thread could add records after we get the size and before we add the new records
			val baseOffset = allRecords.size
			allRecords.addAll(newRecords)

			return ProduceResponse.PartitionResponse(0, baseOffset.toLong())
		}

		val responses = produceRequest.partitionRecords()!!.entries
				.filterNotNull()
				.filter { it.key != null }
				.filter { it.key.topic() != null }
				.filter { it.value != null }
				.associateBy({ it.key }, { toPartitionResponse(it.key.topic(), it.key.partition(), it.value) })
		if (requestHeader.apiVersion() == 0.toShort())
			ProduceResponse(responses)
		else
			ProduceResponse(responses, 0)
	}

	/**
	 * The handler for fetch requests.
	 *
	 * Default Behavior: respond with all messages published to the requested topics/partitions, skipping up to the supplied offset
	 */
	var fetchRequestHandler: (RequestHeader, FetchRequest) -> FetchResponse = { requestHeader, fetchRequest ->
		fun toPartitionData(topic: String, partition: Int, startOffset: Long): FetchResponse.PartitionData {
			val messages = internalProducedMessages
					.getOrElse(topic, { ConcurrentHashMap() })
					.getOrElse(partition, { CopyOnWriteArrayList() })
					.drop(startOffset.toInt())
					.filterNotNull();

			val memoryRecords = MemoryRecords.emptyRecords(ByteBuffer.allocate(0), CompressionType.NONE)
			messages.forEachIndexed { i, record ->
				memoryRecords.append(i.toLong() + startOffset, record)
			}
			memoryRecords.close()

			return FetchResponse.PartitionData(0, messages.size.toLong(), memoryRecords.buffer())
		}

		val responses = fetchRequest.fetchData().entries
				.asSequence()
				.filterNotNull()
				.filter { it.key != null }
				.filter { it.value != null }
				.filter { it.key.topic() != null }
				.associateBy({ it.key }, { toPartitionData(it.key.topic(), it.key.partition(), it.value.offset) });

		if (requestHeader.apiVersion() == 0.toShort())
			FetchResponse(responses)
		else
			FetchResponse(responses, 0)
	}

	/**
	 * The handler for group coordinator requests.
	 *
	 * Default Behavior: respond with this broker as the coordinator.
	 */
	val groupCoordinatorRequestHandler: (RequestHeader, GroupCoordinatorRequest) -> GroupCoordinatorResponse = { requestHeader, groupCoordinatorRequest ->
		GroupCoordinatorResponse(0, thisBroker)
	}

	/**
	 * The handler for offset fetch requests.
	 *
	 * Default Behavior: respond with all requested topics and partitions currently at offset 0
	 */
	val offsetFetchRequestHandler: (RequestHeader, OffsetFetchRequest) -> OffsetFetchResponse = { requestHeader, offsetFetchRequest ->
		OffsetFetchResponse(offsetFetchRequest.partitions().associateBy({ it }, { OffsetFetchResponse.PartitionData(0, "", 0) }))
	}

	/**
	 * The handler for join group requests.
	 *
	 * Default Behavior: respond with the requestor as the group leader and the first protocol chosen
	 */
	val joinGroupRequestHandler: (RequestHeader, JoinGroupRequest) -> JoinGroupResponse = { requestHeader, joinGroupRequest ->
		val chosenProtocol = joinGroupRequest.groupProtocols()!!.first()!!

		val errorCode = 0.toShort()
		val generationId = 0
		val memberId = "member id"
		val leaderId = "member id"
		val groupProtocol = chosenProtocol.name()
		val groupMembers = mapOf(Pair(memberId, chosenProtocol.metadata()!!))
		JoinGroupResponse(errorCode, generationId, groupProtocol, memberId, leaderId, groupMembers)
	}

	/**
	 * The handler for sync group requests.
	 *
	 * Default Behavior: Return whatever state the requestor provided for itself, or an UNKNOWN_MEMBER_ID if the requestor is not the leader.
	 */
	val syncGroupRequestHandler: (RequestHeader, SyncGroupRequest) -> SyncGroupResponse = handler@ { requestHeader, syncGroupRequest ->
		val errorCode = 0.toShort()
		val memberState = syncGroupRequest.groupAssignment()!![syncGroupRequest.memberId()] ?: return@handler SyncGroupResponse(25, ByteBuffer.allocate(0))
		SyncGroupResponse(errorCode, memberState)
	}

	/**
	 * The handler for heartbeat requests.
	 *
	 * Default Behavior: Return success.
	 */
	val heartbeatRequestHandler: (RequestHeader, HeartbeatRequest) -> HeartbeatResponse = { requestHeader, heartbeatRequest ->
		HeartbeatResponse(0)
	}

	/**
	 * The handler for leave group requests.
	 *
	 * Default Behavior: Return success.
	 */
	val leaveGroupRequestHandler: (RequestHeader, LeaveGroupRequest) -> LeaveGroupResponse = { requestHeader, leaveGroupRequest ->
		LeaveGroupResponse(0)
	}

	/**
	 * The handler for offset commit requests.
	 *
	 * Default Behavior: Return success for all topics/partitions submitted.
	 */
	val offsetCommitRequestHandler: (RequestHeader, OffsetCommitRequest) -> OffsetCommitResponse = { requestHeader, offsetCommitRequest ->
		OffsetCommitResponse(offsetCommitRequest.offsetData().keys.associateBy({ it }, { 0.toShort() }))
	}

	private val lengthPrefixedMessageServer = LengthPrefixedMessageServer { processRequest(it) }
	private var brokers: Set<Node> = setOf(thisBroker)
	private var partitions: ConcurrentMap<String, ConcurrentMap<Int, PartitionInfo>> = ConcurrentHashMap()
	private var internalProducedMessages = ConcurrentHashMap<String, ConcurrentMap<Int, CopyOnWriteArrayList<Record>>>()

	/**
	 * All of the messages that have been produced to this broker for the given topic/partition.  The indexes are the offsets.
	 */
	fun getProducedMessages(topic: String, partition: Int): List<KeyAndMessage> {
		val topicMessages = internalProducedMessages[topic] as Map<Int, Collection<Record>>? ?: emptyMap()
		val partitionMessages = topicMessages[partition] ?: emptyList()
		return partitionMessages.map { KeyAndMessage(it.key()?.copyToArray(), it.value()?.copyToArray())}
	}

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
	fun addBroker(broker: Node) {
		brokers += broker
	}

	/**
	 * Resets this broker to its default state where it only knows about itself.
	 */
	fun clearBrokers() {
		brokers = setOf(thisBroker)
	}

	/**
	 * Adds a topic to this broker.  This will create one partition for the topic whose leader will be this broker.  Returned as part of metadata requests.
	 */
	fun addTopic(topicName: String) {
		addPartition(PartitionInfo(topicName, 0, thisBroker, emptyArray(), emptyArray()))
	}

	/**
	 * Adds a partition to this broker.  This can be a partition for a new topic or an existing one.  If the partition already existed, this will overwrite it.
	 */
	fun addPartition(partitionInfo: PartitionInfo) {
		partitions.computeIfAbsent(partitionInfo.topic(), { ConcurrentHashMap() })
				.put(partitionInfo.partition(), partitionInfo)
	}

	/**
	 * Clears all topics from this broker.
	 */
	fun clearTopics() {
		partitions = ConcurrentHashMap()
	}

	/**
	 * Clears all messages produced to this broker.
	 */
	fun clearMessages() {
		internalProducedMessages = ConcurrentHashMap<String, ConcurrentMap<Int, CopyOnWriteArrayList<Record>>>()
	}

	private fun processRequest(byteBuffer: ByteBuffer): ByteBuffer {
		val requestHeader = RequestHeader.parse(byteBuffer)
		val requestBody = AbstractRequest.getRequest(requestHeader.apiKey().toInt(), requestHeader.apiVersion().toInt(), byteBuffer)
		when (requestBody ) {
			is MetadataRequest -> return metadataRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is ProduceRequest -> return produceRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is FetchRequest -> return fetchRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is GroupCoordinatorRequest -> return groupCoordinatorRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is OffsetFetchRequest -> return offsetFetchRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is JoinGroupRequest -> return joinGroupRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is SyncGroupRequest -> return syncGroupRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is HeartbeatRequest -> return heartbeatRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is LeaveGroupRequest -> return leaveGroupRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is OffsetCommitRequest -> return offsetCommitRequestHandler(requestHeader, requestBody).toResponseByteBuffer(requestHeader.correlationId())
			is ListOffsetRequest, is LeaderAndIsrRequest, is StopReplicaRequest, is ControlledShutdownRequest, is UpdateMetadataRequest, is DescribeGroupsRequest, is ListGroupsRequest -> {
				throw UnsupportedOperationException("Unhandled request type.")
			}
			else -> throw UnsupportedOperationException("Unhandled request type.")
		}
	}

	data class KeyAndMessage(val key: ByteArray?, val message: ByteArray?)
}
