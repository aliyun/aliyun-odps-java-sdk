package com.aliyun.odps.utils;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Instance;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

/**
 * @author haoxiang.mhx@alibaba-inc.com
 *
 * A gson builder SINGLETON that can return a gson object with proper settings.
 * The gson object provided by this builder can deal with Object type
 * in a Map well, especially when encoutering Double/Long in fact but
 * declared as Object.
 */
public class GsonObjectBuilder {

  /**
   * An adapter which fixes Gson's drawback. It can deal with
   * Double/Long well.
   */
  public static class GsonObjectAdapter extends TypeAdapter<Object> {

    // Object TypeAdapter Delegate
    private final TypeAdapter<Object> delegate = new Gson().getAdapter(Object.class);

    @Override public Object read(JsonReader in) throws IOException {
      JsonToken token = in.peek();
      switch (token) {
        case BEGIN_ARRAY:
          List<Object> list = new ArrayList<Object>();
          in.beginArray();
          while (in.hasNext()) {
            list.add(read(in));
          }
          in.endArray();
          return list;

        case BEGIN_OBJECT:
          Map<String, Object> map = new LinkedTreeMap<String, Object>();
          in.beginObject();
          while (in.hasNext()) {
            map.put(in.nextName(), read(in));
          }
          in.endObject();
          return map;

        case STRING:
          return in.nextString();

        // two difference cases: Double & Long
        case NUMBER:
          String inStr = in.nextString();
          if (inStr.contains(".") || inStr.contains("e") || inStr.contains("E")) {
            return Double.parseDouble(inStr);
          } else {
            // for safety, check the overflow issue, return BigInteger when overflow occurs
            BigInteger num = new BigInteger(inStr);
            BigInteger longMax = new BigInteger(String.valueOf(Long.MAX_VALUE));
            BigInteger longMin = new BigInteger(String.valueOf(Long.MIN_VALUE));
            if (num.compareTo(longMax) > 0 || num.compareTo(longMin) < 0) {
              return num;
            }
          }
          return Long.parseLong(inStr);

        case BOOLEAN:
          return in.nextBoolean();

        case NULL:
          in.nextNull();
          return null;

        default:
          throw new IllegalStateException();
      }
    }

    @SuppressWarnings("unchecked")
    @Override public void write(JsonWriter out, Object value) throws IOException {
      if (value == null) {
        out.nullValue();
        return;
      }

      delegate.write(out, value);
    }
  }

  public static class TaskSummaryAdapter extends TypeAdapter<Object> {

    // Object TypeAdapter Delegate
    private final TypeAdapter<Object> delegate = new Gson().getAdapter(Object.class);

    @Override public Object read(JsonReader in) throws IOException {
      JsonToken token = in.peek();
      switch (token) {
        case BEGIN_ARRAY:
          List<Object> list = new ArrayList<Object>();
          in.beginArray();
          while (in.hasNext()) {
            list.add(read(in));
          }
          in.endArray();
          return list;

        case BEGIN_OBJECT:
          Instance.TaskSummary ts = new Instance.TaskSummary();
          in.beginObject();
          while (in.hasNext()) {
            ts.put(in.nextName(), read(in));
          }
          in.endObject();
          return ts;

        case STRING:
          return in.nextString();

        case NUMBER:
          String inStr = in.nextString();
          if (inStr.contains(".") || inStr.contains("e") || inStr.contains("E")) {
            return Double.parseDouble(inStr);
          } else {
            // for safety, check the overflow issue, return BigInteger when overflow occurs
            BigInteger num = new BigInteger(inStr);
            BigInteger longMax = new BigInteger(String.valueOf(Long.MAX_VALUE));
            BigInteger longMin = new BigInteger(String.valueOf(Long.MIN_VALUE));
            if (num.compareTo(longMax) > 0 || num.compareTo(longMin) < 0) {
              return num;
            }
          }
          return Long.parseLong(inStr);

        case BOOLEAN:
          return in.nextBoolean();

        case NULL:
          in.nextNull();
          return null;

        default:
          throw new IllegalStateException();
      }
    }

    @SuppressWarnings("unchecked")
    @Override public void write(JsonWriter out, Object value) throws IOException {
      if (value == null) {
        out.nullValue();
        return;
      }

      delegate.write(out, value);
    }
  }

  private GsonObjectBuilder() {

  }

  private static class GsonHolder {
    private static final GsonObjectAdapter ADAPTER = new GsonObjectAdapter();
    private static final TaskSummaryAdapter TS_ADAPTER = new TaskSummaryAdapter();
    private static final Gson GSON_INSTANCE = new GsonBuilder()
            .disableHtmlEscaping()
            .registerTypeAdapter(Map.class, ADAPTER)
            .registerTypeAdapter(new TypeToken<Map<String, Object>>(){}.getType(), ADAPTER)
            .registerTypeAdapter(new TypeToken<Map<Object, Object>>(){}.getType(), ADAPTER)
            .registerTypeAdapter(Instance.TaskSummary.class, TS_ADAPTER)
            .create();
  }

  public static Gson get() {
    return GsonHolder.GSON_INSTANCE;
  }
}
