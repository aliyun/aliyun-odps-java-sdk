package com.aliyun.odps.ml;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

@Root(name = "Onlinemodel", strict = false)
public class ModelAbTestInfo{
	
	@Element(name = "Project", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String project;
	
	@Element(name = "Name", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String modelName;
	
	@Element(name = "ABTest", required = false)
	public ModelAbTestConf abTestConf;

}
