package com.zoltu.extensions

import kafka.api.ProducerResponse
import kafka.api.TopicMetadataResponse
import java.nio.ByteBuffer

fun TopicMetadataResponse.toByteBuffer(): ByteBuffer {
	return toByteBuffer(this.sizeInBytes(), { byteBuffer -> this.writeTo(byteBuffer) })
}

fun ProducerResponse.toByteBuffer(): ByteBuffer {
	return toByteBuffer(this.sizeInBytes(), { byteBuffer -> this.writeTo(byteBuffer) })
}

private fun toByteBuffer(sizeInBytes: Int, writeTo: (ByteBuffer) -> Unit) : ByteBuffer {
	val buffer = ByteBuffer.wrap(ByteArray(sizeInBytes))
	writeTo(buffer)
	return buffer
}
