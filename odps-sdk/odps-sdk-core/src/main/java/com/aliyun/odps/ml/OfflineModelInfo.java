package com.aliyun.odps.ml;

import com.aliyun.odps.rest.SimpleXmlUtils;
import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;
import com.aliyun.odps.simpleframework.xml.convert.Convert;

@Root(name = "Offlinemodel", strict = false)
public class OfflineModelInfo {
	@Element(name = "Name", required = false)
    @Convert(SimpleXmlUtils.EmptyStringConverter.class)
    public String modelName;

	@Element(name = "ModelPath", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String modelPath;

	@Element(name = "Rolearn", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String rolearn;

	@Element(name = "Type", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String type;

	@Element(name = "Version", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String version;

	@Element(name = "Processor", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String processor;

	@Element(name = "Configuration", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String configuration;

	@Element(name = "SrcProject", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String srcProject;

	@Element(name = "SrcModel", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String srcModel;

	@Element(name = "DestProject", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String destProject;

	@Element(name = "DestModel", required = false)
	@Convert(SimpleXmlUtils.EmptyStringConverter.class)
	public String destModel;
}
