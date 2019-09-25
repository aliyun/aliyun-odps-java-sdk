package com.aliyun.odps.ml;

import com.aliyun.odps.simpleframework.xml.Element;
import com.aliyun.odps.simpleframework.xml.Root;

@Root(name = "Resource", strict = false)
public class Resource {
	@Element(name = "CPU", required = false)
	public int CPU = 0;

	@Element(name = "Memory", required = false)
	public long memory = 0;

	@Element(name = "GPU", required = false)
	public long GPU = 0;
}
