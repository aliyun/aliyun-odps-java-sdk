package com.aliyun.odps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.StorageTierInfo.StorageTier;
import com.google.gson.JsonObject;

public class StorageTierInfoTest {

  @Test
  public void testColdArchiveTierLookup() {
    assertEquals(StorageTier.COLDARCHIVE,
                 StorageTier.getStorageTierByName("coldarchive"));
    assertEquals(StorageTier.COLDARCHIVE,
                 StorageTier.getStorageTierByName("ColdArchive"));
  }

  @Test
  public void testParseColdArchiveStorageTierInfo() {
    long lastModifiedTimeSeconds = 1_723_456_789L;
    JsonObject tree = new JsonObject();
    tree.addProperty("StorageTier", "coldarchive");
    tree.addProperty("StorageLastModifiedTime", lastModifiedTimeSeconds);

    StorageTierInfo storageTierInfo = StorageTierInfo.getStorageTierInfo(tree);

    assertNotNull(storageTierInfo);
    assertEquals(StorageTier.COLDARCHIVE, storageTierInfo.getStorageTier());
    assertEquals(new Date(lastModifiedTimeSeconds * 1000),
                 storageTierInfo.getStorageLastModifiedTime());
    assertNull(storageTierInfo.getStorageSize(StorageTier.COLDARCHIVE));
    assertNull(StorageTier.COLDARCHIVE.getSizeName());
    assertNull(StorageTier.COLDARCHIVE.getChargeSizeName());
  }

  @Test
  public void testExistingStorageSizeParsing() {
    JsonObject tree = new JsonObject();
    tree.addProperty("StandardSize", 1024L);
    tree.addProperty("LowFrequencySize", 2048L);
    tree.addProperty("LongTermSize", 4096L);

    StorageTierInfo tableStorageTierInfo = StorageTierInfo.getStorageTierInfo(tree);

    assertNotNull(tableStorageTierInfo);
    assertEquals(Long.valueOf(1024L), tableStorageTierInfo.getStorageSize(StorageTier.STANDARD));
    assertEquals(Long.valueOf(2048L),
                 tableStorageTierInfo.getStorageSize(StorageTier.LOWFREQUENCY));
    assertEquals(Long.valueOf(4096L), tableStorageTierInfo.getStorageSize(StorageTier.LONGTERM));

    Map<String, String> projectStorageSizes = new HashMap<>();
    projectStorageSizes.put("chargableAliveDataSize", "8192");
    projectStorageSizes.put("chargableLowFreqStorageSize", "16384");
    projectStorageSizes.put("chargableLongTermStorageSize", "32768");

    StorageTierInfo projectStorageTierInfo =
        StorageTierInfo.getStorageTierInfo(projectStorageSizes);

    assertNotNull(projectStorageTierInfo);
    assertEquals(Long.valueOf(8192L),
                 projectStorageTierInfo.getStorageSize(StorageTier.STANDARD));
    assertEquals(Long.valueOf(16384L),
                 projectStorageTierInfo.getStorageSize(StorageTier.LOWFREQUENCY));
    assertEquals(Long.valueOf(32768L),
                 projectStorageTierInfo.getStorageSize(StorageTier.LONGTERM));
  }
}
