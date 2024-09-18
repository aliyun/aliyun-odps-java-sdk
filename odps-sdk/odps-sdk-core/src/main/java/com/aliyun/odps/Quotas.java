package com.aliyun.odps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Quota.QuotaModel;
import com.aliyun.odps.commons.transport.Params;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.utils.StringUtils;

public class Quotas implements Iterable<Quota> {

  @Root(name = "Quotas", strict = false)
  private static class ListQuotasResponse {

    @ElementList(entry = "Quota", inline = true, required = false)
    private List<QuotaModel> quotas = new ArrayList<>();

    @Element(name = "Marker", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    private String marker;

    @Element(name = "MaxItems", required = false)
    private Integer maxItems;
  }

  private static class QuotaListIterator extends ListIterator<Quota> {

    private Odps odps;
    private Map<String, String> params = new HashMap<>();

    QuotaListIterator(Odps odps, String regionId) {
      this.odps = odps;

      params.put(Params.ODPS_QUOTA_VERSION, Quota.VERSION);
      params.put(Params.ODPS_QUOTA_PROJECT, odps.getDefaultProject());
      if (regionId != null) {
        params.put(Params.ODPS_QUOTA_REGION_ID, regionId);
      }
    }

    @Override
    protected List<Quota> list() {
      ArrayList<Quota> quotas = new ArrayList<>();

      String lastMarker = params.get("marker");
      if (params.containsKey("marker") && StringUtils.isNullOrEmpty(lastMarker)) {
        return null;
      }

      try {
        ListQuotasResponse resp = odps.getRestClient().request(
            ListQuotasResponse.class, "/quotas", "GET", params);

        for (QuotaModel model : resp.quotas) {
          Quota quota = new Quota(odps, model);
          quotas.add(quota);
        }

        params.put("marker", resp.marker);
      } catch (OdpsException e) {
        throw new RuntimeException(e.getMessage(), e);
      }

      return quotas;
    }
  }

  private Odps odps;

  Quotas(Odps odps) {
    if (odps == null) {
      throw new IllegalArgumentException("Argument 'odps' cannot be null");
    }

    this.odps = odps;
  }

  public Quota get(String regionId, String name) {
    if (StringUtils.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Argument 'name' cannot be null or empty");
    }

    return new Quota(odps, regionId, name);
  }

  public Quota getWlmQuota(String project, String name) throws OdpsException {
    if (StringUtils.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Argument 'name' cannot be null or empty");
    }
    if (StringUtils.isNullOrEmpty(project)) {
      throw new IllegalArgumentException("Argument 'project' cannot be null or empty");
    }
    String tenantId = odps.projects().get(project).getTenantId();
    return new Quota(odps, null, name, tenantId);
  }

  public Quota get(String name) {
    return get(null, name);
  }

  public boolean exists(String regionId, String name) throws OdpsException {
    if (StringUtils.isNullOrEmpty(name)) {
      throw new IllegalArgumentException("Argument 'name' cannot be null or empty");
    }

    try {
      new Quota(odps, regionId, name).reload();
      return true;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  public boolean exists(String name) throws OdpsException {
    return exists(null, name);
  }

  public Iterator<Quota> iterator(String regionId) {
    return new QuotaListIterator(odps, regionId);
  }

  @Override
  public Iterator<Quota> iterator() {
    return iterator(null);
  }
}
