package com.aliyun.odps.ml;

import com.aliyun.odps.simpleframework.xml.ElementList;
import com.aliyun.odps.simpleframework.xml.Root;
import java.util.List;

@Root(name = "ABTest", strict = false)
public class ModelAbTestConf{	
	@ElementList(entry = "Item", inline = true, required = false)
	public List<ModelAbTestItem> items;
}
