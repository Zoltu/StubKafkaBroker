package com.zoltu.extensions

import kafka.api.TopicMetadataResponse
import java.nio.ByteBuffer

fun TopicMetadataResponse.toByteBuffer(): ByteBuffer {
	val buffer = ByteBuffer.wrap(ByteArray(this.sizeInBytes()))
	this.writeTo(buffer)
	return buffer
}
