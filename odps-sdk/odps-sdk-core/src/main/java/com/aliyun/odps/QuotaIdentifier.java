package com.aliyun.odps;

import com.aliyun.odps.simpleframework.xml.convert.Converter;
import com.aliyun.odps.simpleframework.xml.stream.InputNode;
import com.aliyun.odps.simpleframework.xml.stream.OutputNode;
import com.google.gson.Gson;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class QuotaIdentifier {


  private String tenantId;
  private String regionId;
  private String nickname;

  public QuotaIdentifier() {
  }

  public QuotaIdentifier(String tenantId, String regionId, String nickname) {
    this.tenantId = tenantId;
    this.regionId = regionId;
    this.nickname = nickname;
  }

  public static QuotaIdentifier of(String tenantId, String regionId, String nickname) {
    return new QuotaIdentifier(tenantId, regionId, nickname);
  }

  // Getters and Setters
  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getRegionId() {
    return regionId;
  }

  public void setRegionId(String regionId) {
    this.regionId = regionId;
  }

  public String getNickname() {
    return nickname;
  }

  public void setNickname(String nickname) {
    this.nickname = nickname;
  }


  public static class QuotaIdentifierConverter implements Converter<QuotaIdentifier> {

    private final Gson gson = new Gson();

    @Override
    public QuotaIdentifier read(InputNode node) throws Exception {
      String json = node.getValue();
      return gson.fromJson(json, QuotaIdentifier.class);
    }

    @Override
    public void write(OutputNode node, QuotaIdentifier value) throws Exception {
      if (value.nickname == null && value.regionId == null && value.tenantId == null) {
        node.setValue(""); // 设置空值以生成 <QuotaIdentifier></QuotaIdentifier>
      } else {
        String json = gson.toJson(value);
        node.setValue(json);
      }
    }
  }
}
