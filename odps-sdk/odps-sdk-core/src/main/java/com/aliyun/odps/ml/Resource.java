package com.aliyun.odps.ml;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Resource")
public class Resource {
	@XmlElement(name = "CPU")
	public int CPU = 0;
	
	@XmlElement(name = "Memory")
	public long memory = 0;
}
