package com.aliyun.odps.task;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.google.gson.GsonBuilder;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class KubeTaskTest {
  public static final String ACCESS_ID = "";
  public static final String ACCESS_KEY = "";
  public static final String PROJECT = "";
  public static final String ENDPOINT = "";
  public static Odps odps;
  public static final String TASK_NAME = "kubetask_test";
  public static final String xml =
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                  "<KUBE>\n" +
                  "   <Name>kubetask_test</Name>\n" +
                  "   <Config>\n" +
                  "      <Property>\n" +
                  "         <Name>settings</Name>\n" +
                  "         <Value>{&quot;odps.kube.task.command&quot;:&quot;GET&quot;,&quot;odps.kube.resource.name&quot;:&quot;resource_test&quot;,&quot;odps.kube.resource.kind&quot;:&quot;POD&quot;}</Value>\n" +
                  "      </Property>\n" +
                  "   </Config>\n" +
                  "   <JSON></JSON>\n" +
                  "</KUBE>";

  @BeforeClass
  public static void SetUp() {
    Account account = new AliyunAccount(ACCESS_ID, ACCESS_KEY);
    odps = new Odps(account);
    odps.setDefaultProject(PROJECT);
    odps.setEndpoint(ENDPOINT);
  }

  @Test
  public void testMarshalKubeTask() throws Exception {
    KubeTask task = new KubeTask();
    task.setName(TASK_NAME);
    task.setKubeTaskCommand(KubeTask.Command.GET);
    task.setKubeResourceName("resource_test");
    task.setKubeResourceKind(KubeTask.Kind.POD);
    task.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(task.getSettings()));
    String marshaled = SimpleXmlUtils.marshal(task);
    assertEquals(xml, marshaled);
  }

  @Test
  public void testUnMarshalKubeTask() throws Exception {
    String settings = "{\"odps.kube.task.command\":\"GET\",\"odps.kube.resource.name\":\"resource_test\",\"odps.kube.resource.kind\":\"POD\"}";
    KubeTask task = SimpleXmlUtils.unmarshal(xml.getBytes(), KubeTask.class);
    assertEquals("kubetask_test", task.getName());
    assertEquals(settings, task.getProperties().get("settings"));
  }

  @Test
  public void testKubeTaskCreateCommand() throws FileNotFoundException {
    String yamlFilePath = Objects.requireNonNull(this.getClass().getClassLoader().getResource("yaml/serviceAccount.yaml")).getFile();
    String taskName = "kube_task";
    try {
      Instance instance = KubeTask.run(odps, PROJECT, taskName, yamlFilePath);
      instance.waitForSuccess();
      String logviewUrl = odps.logview().generateLogView(instance, 7 * 24);
      System.out.println(logviewUrl);
      System.out.println(instance.getTaskResults().get(taskName));
    } catch (OdpsException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testKubeTaskGetCommand() {
    String taskName = "kube_task";
    String resourceName = "";

    try {
      Instance instance = KubeTask.run(odps, PROJECT, taskName, null, resourceName, null, KubeTask.Command.GET, KubeTask.Kind.ROLE);
      instance.waitForSuccess();
      String logviewUrl = odps.logview().generateLogView(instance, 7 * 24).replace("logview.alibaba-inc.com", "daily-logview.alibaba.net");
      System.out.println(logviewUrl);
      System.out.println(instance.getTaskResults().get(taskName));
    } catch (OdpsException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testKubeTaskRemoveCommand() {
    String taskName = "kube_task";
    String appId = "";
    String resourceName = "";

    try {
      Instance instance = KubeTask.run(odps, PROJECT, taskName, appId, resourceName, null, KubeTask.Command.REMOVE, KubeTask.Kind.ROLE);
      instance.waitForSuccess();
      System.out.println(instance.getTaskResults().get(taskName));
    } catch (OdpsException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testKubeTaskRemoveByLabels() {
    String taskName = "kube_task";
    String appId = "";
    String labels = "";

    try {
      Instance instance = KubeTask.run(odps, PROJECT, taskName, appId, null, labels, KubeTask.Command.REMOVE, KubeTask.Kind.PVC);
      instance.waitForSuccess();
      System.out.println(instance.getTaskResults().get(taskName));
    } catch (OdpsException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testKubeTaskRemoveByIdCommand() {
    String taskName = "kube_task";
    String appId = "";

    try {
      Instance instance = KubeTask.run(odps, PROJECT, taskName, appId, KubeTask.Command.REMOVE);
      instance.waitForSuccess();
      System.out.println(instance.getTaskResults().get(taskName));
    } catch (OdpsException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testKubeTaskYamlUtils() throws FileNotFoundException {
    String yamlFilePath = Objects.requireNonNull(this.getClass().getClassLoader().getResource("yaml/serviceAccount.yaml")).getFile();
    System.out.println(KubeTask.loadYamlResources(yamlFilePath));
  }
}
