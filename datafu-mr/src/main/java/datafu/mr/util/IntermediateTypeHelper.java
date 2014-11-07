/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package datafu.mr.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Utility to get the generic types configured by the Mapper and Reducer implementation
 *
 * @author Mathieu Bastian
 */
public class IntermediateTypeHelper {

  @SuppressWarnings("rawtypes")
  public static Type[] getTypes(Class mapperOrReducerClass, Class<?> topLevelClass) {
    if (!topLevelClass.isAssignableFrom(mapperOrReducerClass)) {
      throw new IllegalArgumentException("The input class should inherit from " + topLevelClass.getName());
    }
    Type[] types = null;
    while (types == null && mapperOrReducerClass != null) {
      if (mapperOrReducerClass.getSuperclass() != null && mapperOrReducerClass.getSuperclass().equals(topLevelClass)) {
        types = ((ParameterizedType) mapperOrReducerClass.getGenericSuperclass()).getActualTypeArguments();
      }
      mapperOrReducerClass = mapperOrReducerClass.getSuperclass();
    }
    return types;
  }

  @SuppressWarnings("rawtypes")
  public static Class getMapperInputKeyClass(Class mapperClass) {
    Type[] types = getTypes(mapperClass, Mapper.class);
    if (types != null) {
      Type t = types[0];
      if (t instanceof ParameterizedType) {
        return (Class) ((ParameterizedType) t).getRawType();
      }
      return (Class) t;
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static Class getMapperInputValueClass(Class mapperClass) {
    Type[] types = getTypes(mapperClass, Mapper.class);
    if (types != null) {
      Type t = types[1];
      if (t instanceof ParameterizedType) {
        return (Class) ((ParameterizedType) t).getRawType();
      }
      return (Class) t;
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static Class getMapperOutputKeyClass(Class mapperClass) {
    Type[] types = getTypes(mapperClass, Mapper.class);
    if (types != null) {
      Type t = types[2];
      if (t instanceof ParameterizedType) {
        return (Class) ((ParameterizedType) t).getRawType();
      }
      return (Class) t;
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static Class getMapperOutputValueClass(Class mapperClass) {
    Type[] types = getTypes(mapperClass, Mapper.class);
    if (types != null) {
      Type t = types[3];
      if (t instanceof ParameterizedType) {
        return (Class) ((ParameterizedType) t).getRawType();
      }
      return (Class) t;
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static Class getReducerInputKeyClass(Class reducerClass) {
    Type[] types = getTypes(reducerClass, Reducer.class);
    if (types != null) {
      Type t = types[0];
      if (t instanceof ParameterizedType) {
        return (Class) ((ParameterizedType) t).getRawType();
      }
      return (Class) t;
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static Class getReducerInputValueClass(Class reducerClass) {
    Type[] types = getTypes(reducerClass, Reducer.class);
    if (types != null) {
      Type t = types[1];
      if (t instanceof ParameterizedType) {
        return (Class) ((ParameterizedType) t).getRawType();
      }
      return (Class) t;
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static Class getReducerOutputKeyClass(Class reducerClass) {
    Type[] types = getTypes(reducerClass, Reducer.class);
    if (types != null) {
      Type t = types[2];
      if (t instanceof ParameterizedType) {
        return (Class) ((ParameterizedType) t).getRawType();
      }
      return (Class) t;
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static Class getReducerOutputValueClass(Class reducerClass) {
    Type[] types = getTypes(reducerClass, Reducer.class);
    if (types != null) {
      Type t = types[3];
      if (t instanceof ParameterizedType) {
        return (Class) ((ParameterizedType) t).getRawType();
      }
      return (Class) t;
    }
    return null;
  }
}
