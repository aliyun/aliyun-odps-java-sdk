package com.aliyun.odps;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonObject;

public class StorageTierInfo {

  public enum StorageTier {
    /**
     * 分层存储的标准存储(Standard)枚举类型: 标准存储(Standard),低频存储(LowFrequency),长期存储(Longterm)
     */
    STANDARD("Standard", "StandardSize", "chargableAliveDataSize"),
    LOWFREQUENCY("LowFrequency", "LowFrequencySize", "chargableLowFreqStorageSize"),
    LONGTERM("LongTerm", "LongTermSize", "chargableLongTermStorageSize");

    private final String name;
    private final String sizeName;
    private final String chargeSizeName;

    StorageTier(String name, String sizeName, String chargeSizeName) {
      this.name = name;
      this.sizeName = sizeName;
      this.chargeSizeName = chargeSizeName;
    }

    /**
     * 获取当前分层存储类型的名称
     *
     * @return 分层存储类型名称 - name
     */
    public String getName() {
      return name;
    }

    /**
     * 获取从服务端中返回的当前分层存储类型大小对应的字段名,用于table/partition级别
     *
     * @return 分层存储大小的字符串表示
     */
    public String getSizeName() {
      return sizeName;
    }

    /**
     * 获取从服务端中返回的可计量的分层存储大小对应的字段名,用于project级别
     *
     * @return 可计量分层存储大小对应的字符名称
     */
    public String getChargeSizeName() {
      return chargeSizeName;
    }

    /**
     * 根据字符名称得到对应的分层存储的枚举类型
     *
     * @param tier 分层存储类型的字符串
     * @return 字符串对应的枚举类型
     */
    public static StorageTier getStorageTierByName(String tier) {
      for (StorageTier type : StorageTier.values()) {
        if (StringUtils.equals(type.getName().toLowerCase(), tier.toLowerCase())) {
          return type;
        }
      }
      return null;
    }
  }

  private StorageTier storageTier;
  private Date storageLastModifiedTime;// need reload from meta
  private Map<StorageTier, Long> storageSize;

  /**
   * 获取当前的分层存储枚举类型
   *
   * @return 当前分层存储的枚举类型
   */
  public StorageTier getStorageTier() {
    return storageTier;
  }

  /**
   * 获取分层存储上次修改的时间,无修改默认为是表的创建时间
   *
   * @return 分层存储上次修改的时间
   */
  public Date getStorageLastModifiedTime() {
    return storageLastModifiedTime;
  }

  /**
   * 依据分层存储枚举类型的字符名称，得到对应的存储大小
   *
   * @param tier 分层存储枚举类型名称
   * @return 分层存储大小
   */
  public Long getStorageSize(String tier) {
    return getStorageSize(StorageTier.getStorageTierByName(tier));
  }

  /**
   * 根据分层存储的类型，得到对应分层存储类型的大小
   *
   * @param tier 分层存储枚举类型
   * @return 分层存储大小
   */
  public Long getStorageSize(StorageTier tier) {
    if (storageSize == null || tier == null
        || !storageSize.containsKey(tier)) {
      return null;
    }
    return storageSize.get(tier);
  }

  /**
   * 依据 JsonObject 对象, 构造相关的分层存储信息
   *
   * @param tree jsonObject对象
   * @return 被构成出的分层存储对象
   */
  static StorageTierInfo getStorageTierInfo(JsonObject tree) {
    StorageTierInfo result = new StorageTierInfo();
    boolean isNull = true;
    if (tree.has("StorageTier")) {
      isNull = false;
      result.storageTier = StorageTier.getStorageTierByName(tree.get("StorageTier").getAsString());
    }
    if (tree.has("StorageLastModifiedTime")) {
      isNull = false;
      result.storageLastModifiedTime =
          new Date(tree.get("StorageLastModifiedTime").getAsLong() * 1000);
    }
    Map<StorageTier, Long>
        filterSize =
        Arrays.stream(StorageTier.values()).filter(tier -> tree.has(tier.getSizeName())).collect(
            Collectors.toMap(tier -> tier, tier -> tree.get(tier.getSizeName()).getAsLong()));
    if (filterSize.size() != 0) {
      isNull = false;
      result.storageSize = filterSize;
    }
    if (isNull) {
      result = null;
    }
    return result;
  }

  /**
   * 依据Map<String,String> 对象, 构造相关分层存储的对象信息
   *
   * @param map Map<String,String> 对象
   * @return 被构成出的分层存储对象
   */
  static StorageTierInfo getStorageTierInfo(Map<String, String> map) {
    // specially for project extended property
    StorageTierInfo result = null;
    Map<StorageTier, Long>
        filterSize =
        Arrays.stream(StorageTier.values())
            .filter(tier -> map.containsKey(tier.getChargeSizeName()))
            .collect(
                Collectors.toMap(tier -> tier,
                                 tier -> Long.valueOf(map.get(tier.getChargeSizeName()))));
    if (filterSize.size() != 0) {
      result = new StorageTierInfo();
      result.storageSize = filterSize;
    }

    return result;
  }

}
