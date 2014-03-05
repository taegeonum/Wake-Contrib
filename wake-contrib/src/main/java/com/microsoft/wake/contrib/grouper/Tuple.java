package com.microsoft.wake.contrib.grouper;

/**
 * Created by brandon on 3/5/14.
 */
public class Tuple<K, V> {
  private K key;
  private V value;

  public K getKey() { return key; }
  public V getValue() { return value; }
  public Tuple(K key, V value) {
    this.key = key;
    this.value = value;
  }
}
