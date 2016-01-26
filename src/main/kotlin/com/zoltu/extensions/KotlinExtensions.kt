package com.zoltu.extensions

import java.util.ArrayList
import java.util.LinkedHashMap

public inline fun <T, K, V> Sequence<T>.groupBy(keySelector: (T) -> K, valueSelector: (T) -> V): Map<K, List<V>> {
	val map = LinkedHashMap<K, ArrayList<V>>()
	for (element in this) {
		val key = keySelector(element)
		val value = valueSelector(element)
		val list = map.getOrPut(key) { ArrayList<V>() }
		list.add(value)
	}
	return map
}

public inline fun <T, K, V> Iterable<T>.groupBy(keySelector: (T) -> K, valueSelector: (T) -> V): Map<K, List<V>> {
	return this.asSequence().groupBy(keySelector, valueSelector)
}

/**
 * Returns the sum of all values produced by [selector] function applied to each element in the collection.
 */
public inline fun <T> Sequence<T>.sumByLong(selector: (T) -> Long): Long {
	var sum: Long = 0
	for (element in this) {
		sum += selector(element)
	}
	return sum
}
