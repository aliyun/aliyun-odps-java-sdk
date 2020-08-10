package com.aliyun.odps.security;

public class CheckPermissionConstants {
  public enum ObjectType {
    Project,
    Table,
    Function,
    Resource,
    Instance,
  }

  public enum ActionType {
    Read,
    Write,
    List,
    CreateTable,
    CreateInstance,
    CreateFunction,
    CreateResource,
    All,
    Describe,
    Select,
    Alter,
    Update,
    Drop,
    Execute,
    Delete,
    Download
  }

  public enum CheckPermissionResult {
    Allow,
    Deny
  }
}
