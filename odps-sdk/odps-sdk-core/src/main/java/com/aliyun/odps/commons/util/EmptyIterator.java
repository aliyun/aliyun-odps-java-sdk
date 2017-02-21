/**
 * 
 */
package com.aliyun.odps.commons.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * EmptyIterator
 * 
 * @author emerson
 *
 */
public class EmptyIterator<E> implements Iterator<E> {

  private EmptyIterator() {}

  private static final EmptyIterator<Object> EMPTY_ITERATOR = new EmptyIterator<Object>();

  public boolean hasNext() {
    return false;
  }

  public E next() {
    throw new NoSuchElementException();
  }

  public void remove() {
    throw new IllegalStateException();
  }

  public static <T> Iterator<T> emptyIterator() {
    return (Iterator<T>) EmptyIterator.EMPTY_ITERATOR;
  }
}
