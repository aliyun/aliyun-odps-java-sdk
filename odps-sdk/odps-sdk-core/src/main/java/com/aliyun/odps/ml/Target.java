package com.aliyun.odps.ml;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Target")
public class Target{
    @XmlElement(name = "Name")
    public String name;
};
