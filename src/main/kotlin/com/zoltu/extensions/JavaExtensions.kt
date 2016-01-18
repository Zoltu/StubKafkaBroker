package com.zoltu.extensions

import java.nio.ByteBuffer

fun ByteBuffer.copyToArray(): ByteArray {
	val bytes = ByteArray(this.remaining())
	this.get(bytes)
	return bytes
}
