package com.aliyun.odps.ml;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

@Root(name = "BuiltinProcessor", strict = false)
public class BuiltinProcessor extends AbstractProcessor {
	@Element(name = "OfflinemodelProject", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String offlinemodelProject;
	
	@Element(name = "OfflinemodelName", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String offlinemodelName;
}
