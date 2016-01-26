package com.zoltu.extensions

import kafka.api.FetchResponse
import kafka.api.FetchResponseSend
import kafka.api.RequestOrResponse
import java.nio.ByteBuffer
import java.nio.channels.GatheringByteChannel

fun RequestOrResponse.toByteBuffer(): ByteBuffer {
	return toByteBuffer(this.sizeInBytes(), { byteBuffer -> this.writeTo(byteBuffer) })
}

fun FetchResponse.toByteBuffer(): ByteBuffer {
	val fetchResponseSend = FetchResponseSend("foo", this)
	val gatheringByteChannel = InMemoryGatheringByteChannel()
	fetchResponseSend.writeTo(gatheringByteChannel)
	// fetchResponseSend includes the length bytes but we will include that ourselves later, so skip the first 4 bytes
	val buffer = gatheringByteChannel.toByteBuffer(4)
	return buffer
}

private class InMemoryGatheringByteChannel() : GatheringByteChannel {
	@Volatile
	private var open = true
	private var bytes = emptySequence<Byte>()

	fun toByteBuffer(offset: Int): ByteBuffer {
		val result = ByteBuffer.wrap(bytes.drop(offset).toArrayList().toByteArray())
		result.position(result.limit())
		return result
	}

	override fun isOpen(): Boolean {
		return open;
	}

	override fun close() {
		open = false
	}

	override fun write(sources: Array<out ByteBuffer>?, offset: Int, length: Int): Long {
		sources!!

		return sources
				.asSequence()
				.drop(offset)
				.take(length)
				.sumByLong { source -> write(source).toLong() }
	}

	override fun write(sources: Array<out ByteBuffer>?): Long {
		sources!!

		return sources
				.asSequence()
				.sumByLong { source -> write(source).toLong() }
	}

	override fun write(source: ByteBuffer?): Int {
		source!!

		val remaining = source.remaining()
		val byteArray = ByteArray(remaining)
		source.get(byteArray)
		bytes = bytes.plus(byteArray.asSequence())
		return remaining
	}
}

private fun toByteBuffer(sizeInBytes: Int, writeTo: (ByteBuffer) -> Unit) : ByteBuffer {
	val buffer = ByteBuffer.wrap(ByteArray(sizeInBytes))
	writeTo(buffer)
	return buffer
}
