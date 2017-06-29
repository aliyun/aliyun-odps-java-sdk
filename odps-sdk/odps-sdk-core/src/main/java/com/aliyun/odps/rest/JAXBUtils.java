/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.rest;

import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import com.aliyun.odps.Task;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;

/**
 * Utilities for JAXB marshal and unmarshal
 */
public class JAXBUtils {

  static {
    injectInternalTasks();
  }

  private static final ThreadLocal<Map<String, JAXBContext>>
      threadLocal =
      new ThreadLocal<Map<String, JAXBContext>>() {
        @Override
        protected Map<String, JAXBContext> initialValue() {
          return new HashMap<String, JAXBContext>();
        }
      };

  private static class CDATAXMLStreamWriter implements XMLStreamWriter {

    private static final Pattern XML_CHARS = Pattern.compile("[&<>]");

    private XMLStreamWriter w;

    public CDATAXMLStreamWriter(XMLStreamWriter writer) {
      w = writer;
    }

    @Override
    public void close() throws XMLStreamException {
      w.close();
    }

    @Override
    public void flush() throws XMLStreamException {
      w.flush();
    }

    @Override
    public NamespaceContext getNamespaceContext() {
      return w.getNamespaceContext();
    }

    @Override
    public String getPrefix(String uri) throws XMLStreamException {
      return w.getPrefix(uri);
    }

    @Override
    public Object getProperty(String name) throws IllegalArgumentException {
      return w.getProperty(name);
    }

    @Override
    public void setDefaultNamespace(String uri) throws XMLStreamException {
      w.setDefaultNamespace(uri);
    }

    @Override
    public void setNamespaceContext(NamespaceContext context)
        throws XMLStreamException {
      w.setNamespaceContext(context);
    }

    @Override
    public void setPrefix(String prefix, String uri) throws XMLStreamException {
      w.setPrefix(prefix, uri);
    }

    @Override
    public void writeAttribute(String prefix, String namespaceURI,
                               String localName, String value) throws XMLStreamException {
      w.writeAttribute(prefix, namespaceURI, localName, value);
    }

    @Override
    public void writeAttribute(String namespaceURI, String localName,
                               String value) throws XMLStreamException {
      w.writeAttribute(namespaceURI, localName, value);
    }

    @Override
    public void writeAttribute(String localName, String value)
        throws XMLStreamException {
      w.writeAttribute(localName, value);
    }

    @Override
    public void writeCData(String data) throws XMLStreamException {
      w.writeCData(data);
    }

    @Override
    public void writeCharacters(char[] text, int start, int len)
        throws XMLStreamException {
      w.writeCharacters(new String(text, start, len));
    }

    @Override
    public void writeCharacters(String text) throws XMLStreamException {
      boolean useCData = XML_CHARS.matcher(text).find();
      if (useCData) {
        w.writeCData(text);
      } else {
        w.writeCharacters(text);
      }
    }

    @Override
    public void writeComment(String data) throws XMLStreamException {
      w.writeComment(data);
    }

    @Override
    public void writeDTD(String dtd) throws XMLStreamException {
      w.writeDTD(dtd);
    }

    @Override
    public void writeDefaultNamespace(String namespaceURI)
        throws XMLStreamException {
      w.writeDefaultNamespace(namespaceURI);
    }

    @Override
    public void writeEmptyElement(String prefix, String localName,
                                  String namespaceURI) throws XMLStreamException {
      w.writeEmptyElement(prefix, localName, namespaceURI);
    }

    @Override
    public void writeEmptyElement(String namespaceURI, String localName)
        throws XMLStreamException {
      w.writeEmptyElement(namespaceURI, localName);
    }

    @Override
    public void writeEmptyElement(String localName) throws XMLStreamException {
      w.writeEmptyElement(localName);
    }

    @Override
    public void writeEndDocument() throws XMLStreamException {
      w.writeEndDocument();
    }

    @Override
    public void writeEndElement() throws XMLStreamException {
      w.writeEndElement();
    }

    @Override
    public void writeEntityRef(String name) throws XMLStreamException {
      w.writeEntityRef(name);
    }

    @Override
    public void writeNamespace(String prefix, String namespaceURI)
        throws XMLStreamException {
      w.writeNamespace(prefix, namespaceURI);
    }

    @Override
    public void writeProcessingInstruction(String target, String data)
        throws XMLStreamException {
      w.writeProcessingInstruction(target, data);
    }

    @Override
    public void writeProcessingInstruction(String target)
        throws XMLStreamException {
      w.writeProcessingInstruction(target);
    }

    @Override
    public void writeStartDocument() throws XMLStreamException {
      w.writeStartDocument();
    }

    @Override
    public void writeStartDocument(String encoding, String version)
        throws XMLStreamException {
      w.writeStartDocument(encoding, version);
    }

    @Override
    public void writeStartDocument(String version) throws XMLStreamException {
      w.writeStartDocument(version);
    }

    @Override
    public void writeStartElement(String prefix, String localName,
                                  String namespaceURI) throws XMLStreamException {
      w.writeStartElement(prefix, localName, namespaceURI);
    }

    @Override
    public void writeStartElement(String namespaceURI, String localName)
        throws XMLStreamException {
      w.writeStartElement(namespaceURI, localName);
    }

    @Override
    public void writeStartElement(String localName) throws XMLStreamException {
      w.writeStartElement(localName);
    }

  }

  public static <T> String marshal(T obj, Class<T> clazz) throws JAXBException {
    JAXBContext jc = getJAXBContext(clazz);
    Marshaller m = jc.createMarshaller();

    StringWriter out = new StringWriter();

    try {
      XMLOutputFactory xof = XMLOutputFactory.newInstance();
      XMLStreamWriter writer = new CDATAXMLStreamWriter(
          xof.createXMLStreamWriter(out));
      m.marshal(obj, writer);
      writer.flush();
    } catch (XMLStreamException e) {
      throw new RuntimeException(e);
    }

    return out.toString();
  }

  public static <T> T unmarshal(byte[] xml, Class<T> clazz) throws JAXBException {
    if (xml == null) {
      throw new RuntimeException("Invalid XML to unmarshal.");
    }

    JAXBContext jc = getJAXBContext(clazz);
    Unmarshaller um = jc.createUnmarshaller();
    return (T) um.unmarshal(new ByteArrayInputStream(xml));
  }

  @SuppressWarnings("unchecked")
  public static <T> T unmarshal(Response resp, Class<T> clazz)
      throws JAXBException {
    if (resp == null || resp.getBody() == null) {
      throw new RuntimeException("Invalid XML to unmarshal.");
    }
    return unmarshal(resp.getBody(), clazz);
  }

  private static JAXBContext getJAXBContext(Class<?> clazz)
      throws JAXBException {
    Map<String, JAXBContext> cache = threadLocal.get();
    JAXBContext jc = cache.get(clazz.getName());
    if (jc == null) {
      jc = JAXBContext.newInstance(clazz);
      cache.put(clazz.getName(), jc);
    }
    return jc;
  }

  public static class EpochBinding extends DateBinding {
    @Override
    public Date unmarshal(String v) {
      try {
        return new Date(Long.parseLong(v) * 1000);
      } catch (Exception e) {
        return null;
      }
    }
  }

  public static class DateBinding extends XmlAdapter<String, Date> {

    @Override
    public String marshal(Date date) throws Exception {
      return DateUtils.formatRfc822Date(date);
    }

    @Override
    public Date unmarshal(String date) throws Exception {
      return DateUtils.parseRfc822Date(date);
    }
  }

  private static void injectInternalTasks() {
    XmlSeeAlso annotation = Task.class.getAnnotation(XmlSeeAlso.class);
    Class<?>[] oldValue = annotation.value();
    Class<?>[] newValue = Arrays.copyOf(oldValue, oldValue.length + 1);
    try {
      newValue[oldValue.length] = Class.forName("com.aliyun.odps.task.InternalTask");
      Object handler = Proxy.getInvocationHandler(annotation);
      Field f = handler.getClass().getDeclaredField("memberValues");
      f.setAccessible(true);
      Map<String, Object> memberValues = (Map<String, Object>) f.get(handler);
      memberValues.put("value", newValue);
    } catch (ClassNotFoundException e1) {
      // If class com.aliyun.odps.task.InternalTask not found
      // DO Nothing
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    }
  }
}
