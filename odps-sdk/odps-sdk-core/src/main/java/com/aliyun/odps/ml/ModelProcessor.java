package com.aliyun.odps.ml;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

@Root(name = "Processor", strict = false)
public class ModelProcessor {
	@Element(name = "Id", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String className;
	
	@Element(name = "LibName", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String libName;
	
	@Element(name = "RefResource", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String refResource;
	
	@Element(name = "Configuration", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String configuration;
}
