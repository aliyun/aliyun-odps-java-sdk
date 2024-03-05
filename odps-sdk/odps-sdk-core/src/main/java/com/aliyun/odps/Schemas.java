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

package com.aliyun.odps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Schema.SchemaModel;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.utils.ExceptionUtils;
import com.aliyun.odps.utils.StringUtils;

public class Schemas implements Iterable<Schema> {

  private final RestClient client;
  private final Odps odps;

  @Root(name = "Schemas", strict = false)
  static class ListSchemasResponse {

    @ElementList(entry = "Schema", inline = true, required = false)
    List<SchemaModel> schemas = new ArrayList<>();

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    String marker;

    @Element(name = "MaxItems", required = false)
    Integer maxItems;
  }

  Schemas(Odps odps) {
    this.odps = odps;
    this.client = odps.getRestClient();
  }

  /**
   * Returns the current schema set by {@link com.aliyun.odps.Odps#setCurrentSchema(String)}
   */
  public Schema get() throws OdpsException {
    if (!StringUtils.isNullOrEmpty(odps.getCurrentSchema())) {
      throw new OdpsException(
          "current schema not set, use Odps#setCurrentSchema(String) to set current schema");
    }

    return get(odps.getDefaultProject(), odps.getCurrentSchema());
  }

  /**
   * Returns the requested schema
   *
   * @param schemaName - name of the requested schema
   */
  public Schema get(String schemaName) {
    return get(odps.getDefaultProject(), schemaName);
  }

  /**
   * Returns the requested schema
   *
   * @param projectName - project name of the requested schema
   * @param schemaName - name of the requested schema
   */
  public Schema get(String projectName, String schemaName) {
    ExceptionUtils.checkStringArgumentNotNull("projectName", projectName);
    ExceptionUtils.checkStringArgumentNotNull("schemaName", schemaName);

    SchemaModel model = new SchemaModel();
    model.name = schemaName;
    return new Schema(model, projectName, odps);
  }

  /**
   * Checks if the schema exists
   *
   * @param schemaName - schema name
   * @return true if the schema exists, false otherwise
   * @throws OdpsException - upon failure
   */
  public boolean exists(String schemaName) throws OdpsException {
    return exists(odps.getDefaultProject(), schemaName);
  }

  /**
   * Checks if the schema exists
   *
   * @param projectName - project name
   * @param schemaName - schema name
   * @return true if the schema exists, false otherwise
   * @throws OdpsException - upon failure
   */
  public boolean exists(String projectName, String schemaName) throws OdpsException {
    Schema schema = get(projectName, schemaName);
    try {
      schema.reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  /**
   * Create a new schema in default project
   *
   * @throws OdpsException - upon failure
   */
  public void create(String schemaName) throws OdpsException {
    create(odps.getDefaultProject(), schemaName);
  }

  /**
   * Create a new schema in specified project
   *
   * @param projectName - the specified project
   * @param schemaName - schema name
   *
   * @throws OdpsException - upon failure
   */
  public void create(String projectName, String schemaName) throws OdpsException {
    create(projectName, schemaName, null, false);
  }

  /**
   * Create a new schema in specified project
   *
   * @param projectName - the specified project
   * @param schemaName - schema name
   * @param comment - schema comment
   * @param ifNotExists - ignore operation if column exists
   * @throws OdpsException - upon failure
   */
  public void create(String projectName,
                     String schemaName,
                     String comment,
                     boolean ifNotExists) throws OdpsException {
    ExceptionUtils.checkStringArgumentNotNull("projectName", projectName);
    ExceptionUtils.checkStringArgumentNotNull("schemaName", schemaName);

    SchemaModel model = new SchemaModel();
    model.project = projectName;
    model.name = schemaName;
    model.comment = comment;
    model.ifNotExists = ifNotExists;

    String resource = ResourceBuilder.buildSchemaResource(projectName);
    Map<String, String> header = new HashMap<>();
    header.put(Headers.CONTENT_TYPE, "application/xml");
    String body;
    try {
      body = SimpleXmlUtils.marshal(model);
    } catch (Exception e) {
      throw new OdpsException(e);
    }
    client.stringRequest(resource, "POST", null, header, body);
  }

  /**
   * Delete target schema in default project
   * throws {@link com.aliyun.odps.NoSuchObjectException} if schema not exists
   *
   * @param schemaName - schema name
   *
   * @exception OdpsException - upon failure
   */
  public void delete(String schemaName) throws OdpsException {
    delete(odps.getDefaultProject(), schemaName);
  }

  /**
   * Delete target schema in the given project.
   * throws {@link com.aliyun.odps.NoSuchObjectException} if schema not exists
   *
   * @param projectName - project name
   * @param schemaName - schema name
   *
   * @exception OdpsException - upon failure
   */
  public void delete(String projectName, String schemaName) throws OdpsException {
    ExceptionUtils.checkStringArgumentNotNull("projectName", projectName);
    ExceptionUtils.checkStringArgumentNotNull("schemaName", schemaName);

    String resource = ResourceBuilder.buildSchemaResource(projectName, schemaName);
    client.request(resource, "DELETE", null, null, null);
  }

  /**
   * Get a schema iterator of the default project.
   *
   * @return A schema iterator.
   */
  @Override
  public Iterator<Schema> iterator() {
    return iterator(odps.getDefaultProject());
  }

  /**
   * Get a schema iterator of the given project.
   *
   * @param projectName Project name.
   * @return A schema iterator.
   */
  public Iterator<Schema> iterator(final String projectName) {
    return new SchemaListIterator(projectName, null);
  }

  /**
   * Get a schema iterator of the given project.
   *
   * @param projectName Project name.
   * @param filter      schema filter
   * @return A schema iterator.
   */
  public Iterator<Schema> iterator(final String projectName, final SchemaFilter filter) {
    return new SchemaListIterator(projectName, filter);
  }

  /**
   * Get a schema iterable of the default project.
   *
   * @return A schema iterable.
   */
  public Iterable<Schema> iterable() {
    return iterable(odps.getDefaultProject());
  }

  /**
   * Get a schema iterable of the given project.
   *
   * @param projectName Project name.
   * @return A schema iterable.
   */
  public Iterable<Schema> iterable(final String projectName) {
    return () -> new SchemaListIterator(projectName, null);
  }

  private class SchemaListIterator extends ListIterator<Schema> {
    String projectName;
    Map<String, String> params = new HashMap<>();
    boolean hasList = false;

    private SchemaFilter filter;

    public SchemaListIterator(final String projectName, SchemaFilter filter) {
      ExceptionUtils.checkStringArgumentNotNull("projectName", projectName);
      this.projectName = projectName;
      this.filter = filter;
    }

    @Override
    public List<Schema> list(String marker, long maxItems) {
      if (marker != null) {
        params.put("marker", marker);
      }
      if (maxItems >= 0) {
        params.put("maxitems", String.valueOf(maxItems));
      }
      return list();
    }

    @Override
    public String getMarker() {
      return params.get("marker");
    }

    @Override
    protected List<Schema> list() {
      ArrayList<Schema> schemas = new ArrayList<>();
      params.put("expectmarker", "true");
      String lastMarker = params.get("marker");
      if (params.containsKey("marker") && StringUtils.isNullOrEmpty(lastMarker)) {
        return null;
      }

      if (filter != null) {
        if (filter.getName() != null) {
          params.put("name", filter.getName());
        }

        if (filter.getOwner() != null) {
          params.put("owner", filter.getOwner());
        }
      }

      String resource = ResourceBuilder.buildSchemaResource(projectName);

      try {

        ListSchemasResponse resp = client.request(
                ListSchemasResponse.class, resource, "GET", params);

        for (SchemaModel model: resp.schemas) {
          Schema schema = new Schema(model, projectName, odps);
          schemas.add(schema);
        }

        params.put("marker", resp.marker);

      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      return schemas;
    }
  }
}
