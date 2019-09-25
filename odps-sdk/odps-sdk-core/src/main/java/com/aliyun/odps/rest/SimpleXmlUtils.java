package com.aliyun.odps.rest;

import com.aliyun.odps.simpleframework.xml.strategy.SimpleTreeStrategy;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Date;

import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.simpleframework.xml.convert.Converter;
import com.aliyun.odps.simpleframework.xml.stream.Format;
import com.aliyun.odps.simpleframework.xml.stream.InputNode;
import com.aliyun.odps.simpleframework.xml.stream.OutputNode;
import com.aliyun.odps.simpleframework.xml.stream.Style;
import com.aliyun.odps.simpleframework.xml.stream.Verbosity;
import com.aliyun.odps.simpleframework.xml.Serializer;
import com.aliyun.odps.simpleframework.xml.convert.AnnotationStrategy;
import com.aliyun.odps.simpleframework.xml.core.Persister;
import com.aliyun.odps.simpleframework.xml.strategy.Strategy;

/**
 * @author: Jon (wanghzong.zw@alibaba-inc.com)
 */
public class SimpleXmlUtils {
    /**
     * Parameters to specify the format of XML files. Format is for global auto-cdata-wrapping.
     */
    private final static int INDENT = 3;
    private final static String PROLOG = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    private final static Style IDENTITY_STYLE = new Style() {
        @Override
        public String getElement(String s) {
            return s;
        }

        @Override
        public String getAttribute(String s) {
            return s;
        }
    };
    private final static Format AUTO_CDATA_FORMAT = new Format(INDENT, PROLOG, IDENTITY_STYLE,
        Verbosity.HIGH, true);

    /**
     * Marshal & Unmalshal methods
     */
    public static <T> String marshal(T obj) throws Exception {
        // Use SimpleTreeStrategy to avoid unwanted 'class' attribute
        Strategy strategy = new AnnotationStrategy(new SimpleTreeStrategy());
        Serializer serializer = new Persister(strategy, AUTO_CDATA_FORMAT);
        StringWriter out = new StringWriter();
        serializer.write(obj, out);
        return out.toString();
    }

    public static <T> void marshal(T obj, OutputNode outputNode) throws Exception {
        Strategy strategy = new AnnotationStrategy(new SimpleTreeStrategy());
        Serializer serializer = new Persister(strategy, AUTO_CDATA_FORMAT);
        serializer.write(obj, outputNode);
    }

    public static <T> T unmarshal(byte[] xml, Class<T> clazz) throws Exception {
        Strategy strategy = new AnnotationStrategy();
        Serializer serializer = new Persister(strategy, AUTO_CDATA_FORMAT);
        return serializer.read(clazz, new ByteArrayInputStream(xml));
    }

    public static <T> T unmarshal(Response resp, Class<T> clazz) throws Exception {
        if (resp == null || resp.getBody() == null) {
            throw new RuntimeException("Invalid XML to unmarshal.");
        }
        return unmarshal(resp.getBody(), clazz);
    }

    public static <T> T unmarshal(InputNode inputNode, Class<T> clazz) throws Exception {
        Strategy strategy = new AnnotationStrategy();
        Serializer serializer = new Persister(strategy, AUTO_CDATA_FORMAT);
        return serializer.read(clazz, inputNode);
    }

    /**
     * Common converter classes
     */
    public static class DateConverter implements Converter<Date> {
        @Override
        public Date read(InputNode inputNode) throws Exception {
            String dateStr = inputNode.getValue();
            try {
                return DateUtils.parseRfc822Date(dateStr);
            } catch (ParseException e) {
                return null;
            } catch (NullPointerException e) {
                return null;
            }
        }

        @Override
        public void write(OutputNode outputNode, Date date) throws Exception {
            outputNode.setValue(DateUtils.formatRfc822Date(date));
            outputNode.commit();
        }
    }

    /**
     * Here, seconds * 1000 turns time in seconds into time in milliseconds.
     */
    public static class EpochConverter implements Converter<Date> {
        @Override
        public void write(OutputNode outputNode, Date date) throws Exception {
            outputNode.setValue(String.valueOf(date.getTime() / 1000));
            outputNode.commit();
        }

        @Override
        public Date read(InputNode node) throws Exception {
            Long seconds = Long.parseLong(node.getValue());
            return new Date(seconds * 1000);
        }
    }

    /**
     * Since JAXB always converts an empty string element to an empty string while SimpleXml
     * converts to null, this converter is introduced to avoid unexpected behaviors.
     */
    public static class EmptyStringConverter implements Converter<String> {
        @Override
        public void write(OutputNode node, String value) throws Exception {
            if (value == null) {
                value = "";
            }
            node.setValue(value);
            node.commit();
        }

        @Override
        public String read(InputNode node) throws Exception {
            String value = node.getValue();
            return value == null ? "" : value;
        }
    }
}