package com.aliyun.odps.ml;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

@Root(name = "Onlinemodel", strict = false)
public class OnlineModelInfoNew {
	@Element(name = "Project", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String project;

	@Element(name = "Name", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String modelName;

	@Element(name = "QOS", required = false)
	public short QOS = 100;

	@Element(name = "InstanceNum", required = false)
	public short instanceNum = 1;

	@Element(name = "Version", required = false)
	public short version = 0;

	@Element(name = "PredictDesc", required = false)
	public ModelPredictDescNew predictDesc;

	@Element(name = "Runtime", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String runtime = "Native";

	@Element(name = "Resource", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public Resource resource;

	@Element(name = "ServiceTag", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String serviceTag;

	@Element(name = "ServiceName", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String serviceName;
}
