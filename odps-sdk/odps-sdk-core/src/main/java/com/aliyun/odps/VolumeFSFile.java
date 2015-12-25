/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aliyun.odps;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request.Method;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.JAXBUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.tunnel.HttpHeaders;
import com.aliyun.odps.tunnel.VolumeFSErrorCode;

/**
 * Volume File System File
 * 
 * @author Emerson Zhao [mailto:zhenyi.zzy@alibaba-inc.com]
 *
 */
public class VolumeFSFile extends LazyLoad implements Iterable<VolumeFSFile> {

  public enum ItemType {
    FILE, DIRECTORY
  }

  public enum ParamKey {
    PATH, REPLICATION, RECURSIVE, MARKER
  }

  public static final String DEFAULT_OWNER = "odps";
  public static final String DEFAULT_GROUP = "odps";
  public static final String DEFAULT_DIRECTORY_PERMISSION = "0777";



  @XmlRootElement(name = "Item")
  static class VolumeFSFileModel {
    @XmlElement(name = "Project")
    String project;
    @XmlElement(name = "Volume")
    String volume;
    @XmlElement(name = "Path")
    String path;
    @XmlElement(name = "Isdir")
    Boolean isdir;
    @XmlElement(name = "Permission")
    String permission;
    @XmlElement(name = "BlockReplications")
    Integer blockReplications;
    @XmlElement(name = "Length")
    Long length;
    @XmlElement(name = "Quota")
    Long quota;
    @XmlElement(name = "BlockSize")
    Long blockSize;
    @XmlElement(name = "Owner")
    String owner;
    @XmlElement(name = "Group")
    String group;
    @XmlElement(name = "CreationTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date creationTime;
    @XmlElement(name = "AccessTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date accessTime;
    @XmlElement(name = "ModificationTime")
    @XmlJavaTypeAdapter(JAXBUtils.DateBinding.class)
    Date modificationTime;
    @XmlElement(name = "Symlink")
    String symlink;

    public String getPath() {
      return path;
    }
  }

  @XmlRootElement(name = "Items")
  private static class VolumeFSList {

    @XmlElement(name = "Item")
    private List<VolumeFSFileModel> files = new ArrayList<VolumeFSFileModel>();

    @XmlElement(name = "Marker")
    private String marker;

    @XmlElement(name = "MaxItems")
    private Integer maxItems;
  }

  @XmlRootElement(name = "Item")
  private static class FileForCreate {
    @XmlElement(name = "Type")
    private String type;
    @XmlElement(name = "Path")
    private String path;

    public void setType(String type) {
      this.type = type;
    }

    public void setPath(String path) {
      this.path = path;
    }
  }

  @XmlRootElement(name = "Item")
  private static class FileForUpdate {
    @XmlElement(name = "Path")
    private String path;
    @XmlElement(name = "Replication")
    private String replication;

    public void setPath(String path) {
      this.path = path;
    }

    public void setReplication(String replication) {
      this.replication = replication;
    }
  }



  private VolumeFSFileModel model;

  private String project;
  private String path;
  private RestClient client;

  protected VolumeFSFile(String project, String path, RestClient client) throws VolumeException {
    checkVolume(path);
    this.project = project;
    this.path = path;
    this.client = client;
  }

  protected VolumeFSFile(VolumeFSFileModel model, String project, String path, RestClient client) {
    this.model = model;
    this.project = project;
    this.path = path;
    this.client = client;
    if (model != null) {
      setLoaded(true);
    }
  }

  /**
   * Get a mocked root path
   * 
   * @return
   */
  public static VolumeFSFile getRoot(String project, Odps odps) {
    RestClient client = odps.getRestClient();
    VolumeFSFileModel model = new VolumeFSFileModel();
    model.project = project;
    model.volume = "";
    model.isdir = true;
    model.blockSize = 0l;
    model.blockReplications = 0;
    model.length = 0l;
    model.owner = DEFAULT_OWNER;
    model.group = DEFAULT_GROUP;
    model.quota = -1l;
    model.permission = DEFAULT_DIRECTORY_PERMISSION;
    model.accessTime = new Date(0);
    model.creationTime = new Date(0);
    model.modificationTime = new Date(0);
    model.symlink = "";
    VolumeFSFile rootPath = new VolumeFSFile(model, project, "/", client);
    return rootPath;
  }

  @Override
  public void reload() throws OdpsException {
    String resource = ResourceBuilder.buildVolumeResource(project, getVolume());
    Map<String, String> params = new HashMap<String, String>();
    Map<String, String> headers = new HashMap<String, String>();
    params.put("meta", null);
    headers.put(HttpHeaders.HEADER_ODPS_VOLUME_FS_PATH, path);
    Response resp = client.request(resource, Method.GET.name(), params, headers, null);
    try {
      model = JAXBUtils.unmarshal(resp, VolumeFSFileModel.class);
    } catch (JAXBException e) {
      throw new OdpsException("Can't bind xml to " + VolumeFSFileModel.class, e);
    }
    setLoaded(true);
  }

  /**
   * Create new file (Only support to create directory now)
   * 
   * @param project
   * @param path
   * @param isDir
   * @param client
   * @return
   * @throws OdpsException
   */
  public static VolumeFSFile create(String project, String path, boolean isDir, RestClient client)
      throws VolumeException {
    checkVolume(path);
    // Maybe change in future
    if (!isDir) {
      throw new VolumeException(VolumeFSErrorCode.NotAcceptableOperation,
          "Not Support to create file yet!");
    }

    FileForCreate file = new FileForCreate();
    file.setPath(path);
    file.setType(isDir ? ItemType.DIRECTORY.name().toLowerCase() : ItemType.FILE.name()
        .toLowerCase());

    String xml = null;
    try {
      xml = JAXBUtils.marshal(file, FileForCreate.class);
    } catch (JAXBException e) {
      throw new VolumeException(e.getMessage(), e);
    }

    Map<String, String> headers = new HashMap<String, String>();
    headers.put(Headers.CONTENT_TYPE, "application/xml");
    String resource = ResourceBuilder.buildVolumeResource(project, getVolume(path));

    try {
      client.stringRequest(resource, Method.POST.name(), null, headers, xml);
    } catch (OdpsException e) {
      throw new VolumeException(e);
    }

    return new VolumeFSFile(project, path, client);
  }

  /**
   * Delete file (or dir)
   * 
   * @param recursive
   * @throws OdpsException
   */
  public void delete(boolean recursive) throws VolumeException {
    Map<String, String> params = new HashMap<String, String>();
    Map<String, String> headers = new HashMap<String, String>();

    params.put(ParamKey.RECURSIVE.name().toLowerCase(), String.valueOf(recursive));
    headers.put(HttpHeaders.HEADER_ODPS_VOLUME_FS_PATH, path);

    String resource = ResourceBuilder.buildVolumeResource(project, getVolume());
    try {
      client.request(resource, Method.DELETE.name(), params, headers, null);
    } catch (OdpsException e) {
      throw new VolumeException(e);
    }
  }

  public VolumeFSFile update(Map<String, String> param) throws VolumeException {
    String src = getPath();
    FileForUpdate file = new FileForUpdate();
    String path = param.get(ParamKey.PATH.name().toLowerCase());
    String replication = param.get(ParamKey.REPLICATION.name().toLowerCase());
    if (path != null) {
      file.setPath(path);
    }
    if (replication != null) {
      file.setReplication(replication);
    }

    String xml = null;
    try {
      xml = JAXBUtils.marshal(file, FileForUpdate.class);
    } catch (JAXBException e) {
      throw new VolumeException(e.getMessage(), e);
    }

    Map<String, String> params = new HashMap<String, String>();
    Map<String, String> headers = new HashMap<String, String>();
    params.put("meta", null);
    headers.put(Headers.CONTENT_TYPE, "application/xml");
    headers.put(HttpHeaders.HEADER_ODPS_VOLUME_FS_PATH, src);
    String resource = ResourceBuilder.buildVolumeResource(project, getVolume(src));

    try {
      client.stringRequest(resource, Method.PUT.name(), params, headers, xml);
    } catch (OdpsException e) {
      throw new VolumeException(e);
    }
    this.model = null;
    this.setLoaded(false);
    if (path != null) {
      this.path = path;
    }
    return this;

  }

  @Override
  public Iterator<VolumeFSFile> iterator() {

    return new ListIterator<VolumeFSFile>() {

      Map<String, String> params = new HashMap<String, String>();
      Map<String, String> headers = new HashMap<String, String>();

      @Override
      protected List<VolumeFSFile> list() {

        List<VolumeFSFile> files = new ArrayList<VolumeFSFile>();
        params.put("expectmarker", "true"); // since sprint-11
        String lastMarker = params.get(ParamKey.MARKER.name().toLowerCase());

        if (params.containsKey(ParamKey.MARKER.name().toLowerCase()) && lastMarker.length() == 0) {
          return null;
        }

        String resource = ResourceBuilder.buildVolumeResource(project, getVolume());
        headers.put(HttpHeaders.HEADER_ODPS_VOLUME_FS_PATH, path);

        try {
          VolumeFSList list =
              client
                  .request(VolumeFSList.class, resource, Method.GET.name(), params, headers, null);

          for (VolumeFSFileModel model : list.files) {
            VolumeFSFile file = new VolumeFSFile(model, project, model.getPath(), client);
            files.add(file);
          }
          params.put(ParamKey.MARKER.name().toLowerCase(), list.marker);
        } catch (OdpsException e) {
          throw new RuntimeException(e.getMessage(), new VolumeException(e));
        }
        return files;
      }
    };
  }


  public String getProject() {
    return this.project;
  }

  public String getVolume() {
    return getVolume(path);
  }

  public static String getVolume(String path) {
    String[] array = path.split("/");
    if (array.length < 2) {
      return null;
    }
    return array[1];
  }

  public String getPath() {
    return this.path;
  }

  public Boolean getIsdir() {
    if (this.model == null || this.model.isdir == null) {
      lazyLoad();
    }
    return this.model.isdir;
  }

  public String getPermission() {
    if (this.model == null || this.model.permission == null) {
      lazyLoad();
    }
    return this.model.permission;
  }

  public Integer getBlockReplications() {
    if (this.model == null || this.model.blockReplications == null) {
      lazyLoad();
    }
    return this.model.blockReplications;
  }

  public Long getLength() {
    if (this.model == null || this.model.length == null) {
      lazyLoad();
    }
    return this.model.length;
  }

  public Long getQuota() {
    if (this.model == null || this.model.quota == null) {
      lazyLoad();
    }
    return this.model.quota;
  }

  public Long getBlockSize() {
    if (this.model == null || this.model.blockSize == null) {
      lazyLoad();
    }
    return this.model.blockSize;
  }

  public String getOwner() {
    if (this.model == null || this.model.owner == null) {
      lazyLoad();
    }
    return this.model.owner;
  }

  public String getGroup() {
    if (this.model == null || this.model.group == null) {
      lazyLoad();
    }
    return this.model.group;
  }

  public Date getCreationTime() {
    if (this.model == null || this.model.creationTime == null) {
      lazyLoad();
    }
    return this.model.creationTime;
  }

  public Date getAccessTime() {
    if (this.model == null || this.model.accessTime == null) {
      lazyLoad();
    }
    return this.model.accessTime;
  }

  public Date getModificationTime() {
    if (this.model == null || this.model.modificationTime == null) {
      lazyLoad();
    }
    return this.model.modificationTime;
  }

  public String getSymlink() {
    if (this.model == null || this.model.symlink == null) {
      lazyLoad();
    }
    return this.model.symlink;
  }

  /**
   * Check path's volume
   * 
   * @param path
   */
  private static void checkVolume(String path) throws VolumeException {
    if (!Volume.checkPathHasVolume(path)) {
      throw new VolumeException(VolumeFSErrorCode.VolumeMissing, "missing volume in path: " + path
          + "!");
    }
  }

  /**
   * Transfer {@link Volume} to {@link VolumeFSFile}
   * 
   * @param project
   * @param volume
   * @return
   */
  public static VolumeFSFile transferVolumeToVolumeFSFile(String project, Volume volume,
      RestClient client) {
    VolumeFSFileModel model = new VolumeFSFileModel();
    model.project = project;
    model.volume = volume.getName();
    model.isdir = true;
    model.blockSize = 0l;
    model.blockReplications = 0;
    model.length = 0l;
    model.owner = DEFAULT_OWNER;
    model.group = DEFAULT_GROUP;
    model.quota = -1l;
    model.permission = DEFAULT_DIRECTORY_PERMISSION;
    model.accessTime = new Date(0);
    model.creationTime = volume.getCreatedTime();
    model.modificationTime = volume.getLastModifiedTime();
    model.symlink = "";
    VolumeFSFile volumeFSFile = new VolumeFSFile(model, project, "/" + volume.getName(), client);
    return volumeFSFile;
  }

}
