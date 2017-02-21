package com.aliyun.odps.ml;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TestBase;
import com.aliyun.odps.commons.transport.OdpsTestUtils;

/**
 * Created by zhenhong.gzh on 16/11/29.
 */
public class OfflineModelTest extends TestBase {
  private static Odps odps;
  private static final String TEST_ACCOUNT = OdpsTestUtils.getProperty("default.project.owner");
  private static final String TEST_ACCOUNT_NEGATIVE = "aliyun$odpstest3@aliyun.com";

  @BeforeClass
  public static void setup() throws OdpsException {
    odps = OdpsTestUtils.newDefaultOdps();
  }

  @Test
  public void testGetModel() throws OdpsException {
    OfflineModels models = odps.offlineModels();
    for (OfflineModel model : models) {
      System.out.println(model.getName());
      assertTrue(model.getOwner() != null);
      assertTrue(model.getProject() != null);
      assertTrue(model.getLastModifiedTime() != null);
      assertTrue(model.getCreatedTime() != null);
      String value = model.getModel();
      assertTrue(value != null);
    }
  }

  @Test
  public void testModelFilter() throws OdpsException {
    OfflineModelFilter filter = new OfflineModelFilter();
    filter.setOwner(TEST_ACCOUNT);
    Iterator<OfflineModel> models = odps.offlineModels().iterator(filter);
    while (models.hasNext()) {
      OfflineModel model = models.next();
      System.out.println(model.getName());
      Assert.assertEquals(TEST_ACCOUNT, model.getOwner());
      assertTrue(model.getProject() != null);
      String value = model.getModel();
      assertTrue(value != null);
    }
  }

  @Test
  public void testModelFilterNegative() throws OdpsException {
    OfflineModelFilter filter = new OfflineModelFilter();
    filter.setOwner(TEST_ACCOUNT_NEGATIVE);
    Iterator<OfflineModel> models = odps.offlineModels().iterator(filter);
    assertFalse(models.hasNext());
  }
}
