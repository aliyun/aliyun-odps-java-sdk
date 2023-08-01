package com.aliyun.odps.task;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.Task;
import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.GsonBuilder;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Commond.CREATE & YamlFilePath combine used.
 * Command.GET/REMOVE/APPEND/UPDATE/LIST 与 ODPS_KUBE_RESOURCE_NAME & ODPS_KUBE_RESOURCE_KIND combine used.
 */
@Root(name = "KUBE", strict = false)
public class KubeTask extends Task {

  /**
   * KUBETASK REQUEST COMMAND
   */
  public enum Command {
    CREATE,
    GET,
    REMOVE,
    APPEND,
    UPDATE,
    LIST
  }

  /**
   * KUBETASK SUPPORT RESOURCE KIND
   */
  public enum Kind {
    NAMESPACE,
    DEPLOYMENT,
    STATEFULSET,
    DAEMONSET,
    REPLICASET,
    SERVICE,
    POD,
    CONFIGMAP,
    SECRET,
    ROLE,
    ROLEBINDING,
    SERVICEACCOUNT,
    PVC,
    PERSISTENTVOLUMECLAIM
  }

  public final static int DEFAULT_PRIORITY = 9;

  public static final String ODPS_KUBE_APP_ID = "odps.kube.app.id";
  public static final String ODPS_KUBE_TASK_COMMAND = "odps.kube.task.command";
  public static final String ODPS_KUBE_RESOURCE_NAME = "odps.kube.resource.name";
  public static final String ODPS_KUBE_RESOURCE_KIND = "odps.kube.resource.kind";
  public static final String ODPS_KUBE_RESOURCE_LABELS = "odps.kube.resource.labels";

  @Element(name = "JSON", required = false)
  @Convert(SimpleXmlUtils.EmptyStringConverter.class)
  private String yamlResources = "";
  private final Map<String, String> settings = new HashMap<>();

  protected KubeTask(){}

  protected KubeTask(String taskName, String yamlFilePath) throws FileNotFoundException {
    super();
    setName(taskName);
    this.yamlResources = loadYamlResources(yamlFilePath);
  }

  public String getYamlResources() {
    return yamlResources;
  }

  public Map<String, String> getSettings() {
    return settings;
  }

  protected void setKubeTaskCommand (Command command) {
    settings.put(ODPS_KUBE_TASK_COMMAND, command.name());
  }

  protected void setKubeResourceName(String name) {
    settings.put(ODPS_KUBE_RESOURCE_NAME, name);
  }

  protected void setKubeResourceKind(Kind kind) {
    settings.put(ODPS_KUBE_RESOURCE_KIND, kind.name());
  }

  protected void setOdpsKubeAppId(String appId) {
    settings.put(ODPS_KUBE_APP_ID, appId);
  }

  protected void setOdpsKubeResourceLabels(String labels) {
    settings.put(ODPS_KUBE_RESOURCE_LABELS, labels);
  }

  /**
   * @param odps {@link Odps}
   * @param project {@link Project}
   * @param taskName Task名称
   * @param yamlPath Yaml文件的路径
   * @return {@link Instance}
   */
  public static Instance run(Odps odps, String project,
                             String taskName, String yamlPath) throws OdpsException, FileNotFoundException {
    return run(odps, project, taskName, yamlPath, DEFAULT_PRIORITY);
  }

  /**
   * @param odps {@link Odps}
   * @param project {@link Project}
   * @param taskName Task名称
   * @param yamlPath Yaml文件的路径
   * @param priority 作业优先级
   * @return {@link Instance}
   */
  public static Instance run(Odps odps, String project,
                             String taskName, String yamlPath,
                             int priority) throws OdpsException, FileNotFoundException {
    KubeTask kubeTask = new KubeTask(taskName, yamlPath);
    kubeTask.setKubeTaskCommand(Command.CREATE);
    kubeTask.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(kubeTask.getSettings()));
    return odps.instances().create(project, kubeTask, priority);
  }

  /**
   * @param odps {@link Odps}
   * @param project {@link Project}
   * @param taskName Task名称
   * @param appId 执行作业的资源ID
   * @param resourceName 执行作业内的资源名称
   * @param labels 资源标签
   * @param command 执行作业资源的操作命令
   * @param kind 执行作业内的资源类型
   * @return {@link Instance}
   */
  public static Instance run(Odps odps, String project, String taskName,
                             String appId, String resourceName, String labels,
                             Command command, Kind kind) throws OdpsException {
    return run(odps, project, taskName, appId, resourceName, labels, command, kind, DEFAULT_PRIORITY);
  }

  /**
   * @param odps {@link Odps}
   * @param project {@link Project}
   * @param taskName Task名称
   * @param appId 执行作业的资源ID
   * @param resourceName 执行作业内的资源名称
   * @param labels 资源标签
   * @param command 执行作业资源的操作命令
   * @param kind 执行作业内的资源类型
   * @param priority 作业优先级
   * @return {@link Instance}
   */
  public static Instance run(Odps odps, String project, String taskName,
                             String appId, String resourceName, String labels,
                             Command command, Kind kind,
                             int priority) throws OdpsException {
    KubeTask kubeTask = new KubeTask();
    kubeTask.setName(taskName);
    if (!StringUtils.isEmpty(appId)) kubeTask.setOdpsKubeAppId(appId);
    if (!StringUtils.isEmpty(resourceName)) kubeTask.setKubeResourceName(resourceName);
    if (!StringUtils.isEmpty(labels)) kubeTask.setOdpsKubeResourceLabels(labels);
    kubeTask.setKubeTaskCommand(command);
    kubeTask.setKubeResourceKind(kind);
    kubeTask.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(kubeTask.getSettings()));
    return odps.instances().create(project, kubeTask, priority);
  }

  /**
   * @param odps {@link Odps}
   * @param project {@link Project}
   * @param taskName Task名称
   * @param command 执行作业资源的操作命令
   * @param appId 执行作业的资源ID
   * @return {@link Instance}
   */
  public static Instance run(Odps odps, String project, String taskName,
                             String appId, Command command) throws OdpsException {
    return run(odps, project, taskName, appId, command, DEFAULT_PRIORITY);
  }

  /**
   * @param odps {@link Odps}
   * @param project {@link Project}
   * @param taskName Task名称
   * @param command 执行作业资源的操作命令
   * @param appId 执行作业的资源ID
   * @param priority 作业优先级
   * @return {@link Instance}
   */
  public static Instance run(Odps odps, String project, String taskName,
                             String appId, Command command, int priority) throws OdpsException {
    KubeTask kubeTask = new KubeTask();
    kubeTask.setName(taskName);
    kubeTask.setOdpsKubeAppId(appId);
    kubeTask.setKubeTaskCommand(command);
    kubeTask.setProperty("settings", new GsonBuilder().disableHtmlEscaping().create().toJson(kubeTask.getSettings()));
    return odps.instances().create(project, kubeTask, priority);
  }

  protected static String loadYamlResources(String yamlFilePath) throws FileNotFoundException {

    Yaml yaml = new Yaml();
    List<String> resources = new ArrayList<>();
    Iterable<Object> iterable = yaml.loadAll(new FileInputStream(yamlFilePath));
    for (Object resource : iterable) {
      resources.add(new GsonBuilder().disableHtmlEscaping().create().toJson(resource));
    }
    return "{" + "\"data\":" + resources + "}";
  }

}
