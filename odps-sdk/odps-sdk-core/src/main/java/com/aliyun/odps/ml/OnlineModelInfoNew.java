package com.aliyun.odps.ml;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Onlinemodel")
public class OnlineModelInfoNew {
	@XmlElement(name = "Project")
	public String project;

	@XmlElement(name = "Name")
	public String modelName;

	@XmlElement(name = "QOS")
	public short QOS = 100;

	@XmlElement(name = "InstanceNum")
	public short instanceNum = 1;

	@XmlElement(name = "Version")
	public short version = 0;

	@XmlElement(name = "PredictDesc")
	public ModelPredictDescNew predictDesc;

	@XmlElement(name = "Runtime")
	public String runtime = "Native";

	@XmlElement(name = "Resource")
	public Resource resource;

	@XmlElement(name = "ServiceTag")
	public String serviceTag;

	@XmlElement(name = "ServiceName")
	public String serviceName;
}
