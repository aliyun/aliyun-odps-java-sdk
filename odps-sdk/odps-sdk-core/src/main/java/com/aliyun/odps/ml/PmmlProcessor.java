package com.aliyun.odps.ml;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

@Root(name = "PmmlProcessor", strict = false)
public class PmmlProcessor extends AbstractProcessor {
	@Element(name = "Pmml", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String pmml;
	
	@Element(name = "RefResource", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String refResource;
	
	@Element(name = "RunMode", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String runMode;
}
