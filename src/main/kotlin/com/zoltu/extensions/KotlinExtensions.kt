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
