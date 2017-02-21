/**
 *
 */
package com.aliyun.odps.ml;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 * 用来构造RESTful API调用的资源标识符
 *
 * @author chao.liu@alibaba-inc.com
 */
public class ModelResourceBuilder {

  public static final String PROJECTS = "/projects";
  public static final String OFFLINEMODELS = "/offlinemodels";
  public static final String ONLINEMODELS = "/onlinemodels";
  public static final String PREDICTIONS = "/predictions";
  public static final String EVALUATIONS = "/evaluations";

  public static String buildOfflineModelResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(OFFLINEMODELS);

    return sb.toString();
  }

  public static String buildOfflineModelResource(String projectName,
                                                 String modelName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(OFFLINEMODELS).append("/").append(modelName);

    return sb.toString();
  }

  public static String buildOnlineModelResource(String projectName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(ONLINEMODELS);

    return sb.toString();
  }

  public static String buildOnlineModelResource(String projectName,
                                                String modelName) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(ONLINEMODELS).append("/").append(modelName);

    return sb.toString();
  }

  public static String buildTrainingResource(String projectName,
                                             String modelName, String trainingId) {
    StringBuilder sb = new StringBuilder();

    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(OFFLINEMODELS).append("/").append(modelName).append("/")
        .append(trainingId);

    return sb.toString();
  }

  public static String buildPredictionResource(String projectName,
                                               String modelName) {
    StringBuilder sb = new StringBuilder();
    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(OFFLINEMODELS).append("/").append(modelName)
        .append(PREDICTIONS);

    return sb.toString();
  }

  public static String buildPredictionResource(String projectName,
                                               String modelName, String predictionName) {
    StringBuilder sb = new StringBuilder();
    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(OFFLINEMODELS).append("/").append(modelName)
        .append(PREDICTIONS).append("/").append(predictionName);

    return sb.toString();
  }

  public static String buildEvaluationResource(String projectName,
                                               String modelName) {
    StringBuilder sb = new StringBuilder();
    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(OFFLINEMODELS).append("/").append(modelName)
        .append(EVALUATIONS);

    return sb.toString();
  }

  public static String buildEvaluationResource(String projectName,
                                               String modelName, String evaluationName) {
    StringBuilder sb = new StringBuilder();
    sb.append(PROJECTS).append('/').append(encodeObjectName(projectName))
        .append(OFFLINEMODELS).append("/").append(modelName)
        .append(EVALUATIONS).append("/").append(evaluationName);

    return sb.toString();
  }


  public static String encodeObjectName(String name) {
    if (name == null || name.trim().length() != name.length()) {
      throw new IllegalArgumentException("Invalid name: " + name);
    }

    return encode(name);
  }

  public static String encode(String str) {
    if (str == null || str.length() == 0) {
      return str;
    }

    String r = null;
    try {
      r = URLEncoder.encode(str, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("Encode failed: " + str);
    }
    r = r.replaceAll("\\+", "%20");
    return r;
  }
}
