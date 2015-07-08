/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.utils;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.aliyun.odps.conf.Configurable;
import com.aliyun.odps.conf.Configuration;
import com.aliyun.odps.io.DataInputBuffer;
import com.aliyun.odps.io.DataOutputBuffer;
import com.aliyun.odps.io.NullWritable;
import com.aliyun.odps.io.Writable;

/**
 * General reflection utilities
 */
public class ReflectionUtils {

  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};

  /**
   * Cache of constructors for each class. Pins the classes so they can't be
   * garbage collected until ReflectionUtils can be collected.
   */
  private static final ConcurrentHashMap<Class<?>, Constructor<?>>
      CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();

  /**
   * Check and set 'configuration' if necessary.
   *
   * @param theObject
   *     object for which to set configuration
   * @param conf
   *     Configuration
   */
  public static void setConf(Object theObject, Configuration conf) {
    if (conf != null) {
      if (theObject instanceof Configurable) {
        ((Configurable) theObject).setConf(conf);
      }
    }
  }

  /**
   * Create an object for the given class and initialize it from conf
   *
   * @param theClass
   *     class of which an object is created
   * @param conf
   *     Configuration
   * @return a new object
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <T> T newInstance(final Class<T> theClass, Configuration conf) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        // privileged code, for this method may be invoked by user code
        Constructor<T> m = (Constructor<T>) AccessController
            .doPrivileged(new PrivilegedAction() {
              public Object run() {
                try {
                  final Constructor<T> m = theClass
                      .getDeclaredConstructor(EMPTY_ARRAY);
                  if (!m.isAccessible()) {
                    m.setAccessible(true);
                  }
                  return m;
                } catch (NoSuchMethodException e) {
                  throw new RuntimeException(e);
                }
              }
            });
        meth = (Constructor<T>) CONSTRUCTOR_CACHE.putIfAbsent(theClass, m);
        if (meth == null) {
          meth = m;
        }
      }
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    setConf(result, conf);
    return result;
  }

  /**
   * Instantiate classes that are ImmmutableClasssesGiraphConfigurable
   *
   * @param theClass
   *     Class to instantiate
   * @param configuration
   *     Graph configuration, may be null
   * @param <T>
   *     Type to instantiate
   * @return Newly instantiated object with configuration set if possible
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstanceFast(Class<T> theClass,
                                      Configuration configuration) {
    T result = null;

    if (theClass.equals(NullWritable.class)) {
      return (T) NullWritable.get();
    }
    try {
      result = theClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    setConf(result, configuration);
    return result;
  }

  // methods to support testing
  static void clearCache() {
    CONSTRUCTOR_CACHE.clear();
  }

  static int getCacheSize() {
    return CONSTRUCTOR_CACHE.size();
  }

  /**
   * Find a declared method in a class
   *
   * @param inClass
   *     Class to search for declared field.
   * @param methodname
   *     Method name to search for
   * @return Method or will return null.
   */
  public static Method findUserClassMethod(final Class<?> inClass, final String methodName) {
    Method m = (Method) AccessController
        .doPrivileged(new PrivilegedAction() {
          public Object run() {
            if (!Object.class.equals(inClass)) {
              for (Method method : inClass.getDeclaredMethods()) {
                if (method.getName().equals(methodName)) {
                  if (!method.isAccessible()) {
                    method.setAccessible(true);
                  }
                  return method;
                }
              }
            }
            return null;
          }
        });
    return m;
  }

  /**
   * Get the underlying class for a type, or null if the type is a variable
   * type.
   *
   * @param type
   *     the type
   * @return the underlying class
   */
  public static Class<?> getClass(Type type) {
    if (type instanceof Class) {
      return (Class<?>) type;
    } else if (type instanceof ParameterizedType) {
      return getClass(((ParameterizedType) type).getRawType());
    } else if (type instanceof GenericArrayType) {
      Type componentType = ((GenericArrayType) type).getGenericComponentType();
      Class<?> componentClass = getClass(componentType);
      if (componentClass != null) {
        return Array.newInstance(componentClass, 0).getClass();
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  /**
   * Try to directly set a (possibly private) field on an Object.
   *
   * @param target
   *     Target to set the field on.
   * @param fieldname
   *     Name of field.
   * @param value
   *     Value to set on target.
   */
  public static void setField(Object target, String fieldname, Object value)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = ReflectionUtils.findDeclaredField(target.getClass(),
                                                    fieldname);
    field.setAccessible(true);
    field.set(target, value);
  }

  /**
   * Find a declared field in a class or one of its super classes
   *
   * @param inClass
   *     Class to search for declared field.
   * @param fieldname
   *     Field name to search for
   * @return Field or will throw.
   * @throws NoSuchFieldException
   *     When field not found.
   */
  public static Field findDeclaredField(Class<?> inClass, String fieldname)
      throws NoSuchFieldException {
    while (!Object.class.equals(inClass)) {
      for (Field field : inClass.getDeclaredFields()) {
        if (field.getName().equals(fieldname)) {
          return field;
        }
      }
      inClass = inClass.getSuperclass();
    }
    String msg = "Class '" + inClass.getName() + "' has no field '" + fieldname
                 + "'";
    throw new NoSuchFieldException(msg);
  }

  /**
   * Find a declared method in a class or one of its super classes
   *
   * @param inClass
   *     Class to search for declared field.
   * @param methodname
   *     Method name to search for
   * @return Method or will throw.
   * @throws NoSuchMethodException
   *     When method not found.
   */
  public static Method findDeclaredMethodRecursive(Class<?> inClass,
                                                   String methodName) throws NoSuchMethodException {
    while (!Object.class.equals(inClass)) {
      for (Method method : inClass.getDeclaredMethods()) {
        if (method.getName().equals(methodName)) {
          return method;
        }
      }
      inClass = inClass.getSuperclass();
    }
    String msg = "ODPS-0730001: Class '" + inClass.getName()
                 + "' not implement method '" + methodName + "'";
    throw new NoSuchMethodException(msg);
  }

  /**
   * Find a declared method in a class
   *
   * @param inClass
   *     Class to search for declared field.
   * @param methodname
   *     Method name to search for
   * @return Method or will throw.
   * @throws NoSuchMethodException
   *     When method not found.
   */
  public static Method findDeclaredMethod(Class<?> inClass, String methodName) {
    if (!Object.class.equals(inClass)) {
      for (Method method : inClass.getDeclaredMethods()) {
        if (method.getName().equals(methodName)) {
          return method;
        }
      }
    }
    String msg = "ODPS-0730001: Class '" + inClass.getName()
                 + "' not implement method '" + methodName + "'";
    throw new RuntimeException(msg);
  }

  /**
   * Get the actual type arguments a child class has used to extend a generic
   * base class.
   *
   * @param <T>
   *     Type to evaluate.
   * @param baseClass
   *     the base class
   * @param childClass
   *     the child class
   * @return a list of the raw classes for the actual type arguments.
   */
  public static <T> List<Class<?>> getTypeArguments(Class<T> baseClass,
                                                    Class<? extends T> childClass) {
    Map<Type, Type> resolvedTypes = new HashMap<Type, Type>();
    Type type = childClass;
    // start walking up the inheritance hierarchy until we hit baseClass
    while (!getClass(type).equals(baseClass)) {
      if (type instanceof Class) {
        // there is no useful information for us in raw types,
        // so just keep going.
        type = ((Class<?>) type).getGenericSuperclass();
      } else {
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Class<?> rawType = (Class<?>) parameterizedType.getRawType();

        Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
        TypeVariable<?>[] typeParameters = rawType.getTypeParameters();
        for (int i = 0; i < actualTypeArguments.length; i++) {
          resolvedTypes.put(typeParameters[i], actualTypeArguments[i]);
        }

        if (!rawType.equals(baseClass)) {
          type = rawType.getGenericSuperclass();
        }
      }
    }

    // finally, for each actual type argument provided to baseClass,
    // determine (if possible) the raw class for that type argument.
    Type[] actualTypeArguments;
    if (type instanceof Class) {
      actualTypeArguments = ((Class<?>) type).getTypeParameters();
    } else {
      actualTypeArguments = ((ParameterizedType) type).getActualTypeArguments();
    }
    List<Class<?>> typeArgumentsAsClasses = new ArrayList<Class<?>>();
    // resolve types by chasing down type variables.
    for (Type baseType : actualTypeArguments) {
      while (resolvedTypes.containsKey(baseType)) {
        baseType = resolvedTypes.get(baseType);
      }
      typeArgumentsAsClasses.add(getClass(baseType));
    }
    return typeArgumentsAsClasses;
  }

  public static void checkNonStaticField(Class<?> userImplClass,
                                         Class<?> parentClass) {
    while (!parentClass.equals(userImplClass)) {
      Field[] fields = userImplClass.getDeclaredFields();
      List<Field> nonStaticFields = new ArrayList<Field>();
      for (Field field : fields) {
        if (!Modifier.isStatic(field.getModifiers())) {
          nonStaticFields.add(field);
        }
      }
      if (!nonStaticFields.isEmpty()) {
        StringBuffer sb = new StringBuffer();
        sb.append("[" + nonStaticFields.get(0).getName());
        for (int i = 1; i < nonStaticFields.size(); ++i) {
          sb.append("," + nonStaticFields.get(i).getName());
        }
        sb.append("]");
        throw new IllegalStateException("ODPS-0730001: Subclass of "
                                        + parentClass.getSimpleName() + " '" + userImplClass
            .getName()
                                        + "' must not have non-static member variables " + sb
                                            .toString()
                                        + ", put them into vertex value if really need.");
      }

      userImplClass = userImplClass.getSuperclass();
    }
  }

  @Deprecated
  public static void cloneWritableInto(Writable dst, Writable src)
      throws IOException {
    CopyInCopyOutBuffer buffer = cloneBuffers.get();
    buffer.outBuffer.reset();
    src.write(buffer.outBuffer);
    buffer.moveData();
    dst.readFields(buffer.inBuffer);
  }

  /**
   * A pair of input/output buffers that we use to clone writables.
   */
  private static class CopyInCopyOutBuffer {

    DataOutputBuffer outBuffer = new DataOutputBuffer();
    DataInputBuffer inBuffer = new DataInputBuffer();

    /**
     * Move the data from the output buffer to the input buffer.
     */
    void moveData() {
      inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
    }
  }

  /**
   * Allocate a buffer for each thread that tries to clone objects.
   */
  private static ThreadLocal<CopyInCopyOutBuffer>
      cloneBuffers =
      new ThreadLocal<CopyInCopyOutBuffer>() {
        protected synchronized CopyInCopyOutBuffer initialValue() {
          return new CopyInCopyOutBuffer();
        }
      };

}
