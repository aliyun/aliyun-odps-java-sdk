package com.aliyun.odps.ml;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

@Root(name = "Item", strict = false)
public class ModelAbTestItem{
	@Element(name = "Project", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String project;
	
	@Element(name = "TargetModel", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String targetModel;
	
	@Element(name = "Pct", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String Pct;
}
