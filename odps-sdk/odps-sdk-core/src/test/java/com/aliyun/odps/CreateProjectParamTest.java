package com.aliyun.odps;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.rest.SimpleXmlUtils;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class CreateProjectParamTest {

  @Test
  public void testDefaultQuota() throws Exception {
    CreateProjectParam param = new CreateProjectParam()
        .name("odps_391238291038")
        .comment("memo")
        .defaultCluster("AT-120N")
        .defaultQuotaId("")
        .owner("odpstest1@aliyun.com")
        .defaultQuota(
            QuotaIdentifier.of("tenant_1", "cn-hangzhou", "test_multi_az_quota_child_1"));

    String xml = SimpleXmlUtils.marshal(param.getProjectModel());

    String expect = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
               + "<Project>\n"
               + "   <Name>odps_391238291038</Name>\n"
               + "   <Comment>memo</Comment>\n"
               + "   <Owner>odpstest1@aliyun.com</Owner>\n"
               + "   <DefaultCluster>AT-120N</DefaultCluster>\n"
               + "   <QuotaID></QuotaID>\n"
               + "   <DefaultQuota>{&quot;tenantId&quot;:&quot;tenant_1&quot;,&quot;regionId&quot;:&quot;cn-hangzhou&quot;,&quot;nickname&quot;:&quot;test_multi_az_quota_child_1&quot;}</DefaultQuota>\n"
               + "</Project>";
    Assert.assertEquals(expect, xml);

    param = new CreateProjectParam()
        .name("aaa")
        .defaultQuota(new QuotaIdentifier());
    xml = SimpleXmlUtils.marshal(param.getProjectModel());

    expect = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
               + "<Project>\n"
               + "   <Name>aaa</Name>\n"
               + "   <DefaultQuota></DefaultQuota>\n"
               + "</Project>";
    Assert.assertEquals(expect, xml);


    String xmlFromServer = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                           + "<Project>\n"
                           + "    <Name>a_test_compute_only_cluster_1</Name>\n"
                           + "    <Type>managed</Type>\n"
                           + "    <Comment></Comment>\n"
                           + "    <State>AVAILABLE</State>\n"
                           + "    <ProjectGroupName></ProjectGroupName>\n"
                           + "    <TenantId></TenantId>\n"
                           + "    <Region>cn-hangzhou</Region>\n"
                           + "    <DefaultQuota>{\"nickname\":\"test_multi_az_quota_child_1\",\"regionId\":\"cn-hangzhou\",\"tenantId\":\"tenant_1\"}</DefaultQuota>\n"
                           + "    <DefaultQuotaNickname>test_multi_az_quota_child_1</DefaultQuotaNickname> <!-- Deprecated -->\n"
                           + "    <DefaultQuotaRegion>cn-hangzhou</DefaultQuotaRegion>"
                           + "    <DefaultQuotaTenant>tenant_1</DefaultQuotaTenant>"
                           + "    <DefaultCluster>AT-120N</DefaultCluster>\n"
                           + "    <Clusters>\n"
                           + "        <Cluster>\n"
                           + "            <Name>AT-120N</Name>\n"
                           + "            <QuotaID></QuotaID>\n"
                           + "        </Cluster>\n"
                           + "    </Clusters>\n"
                           + "</Project>";

    Project.ProjectModel
        unmarshal =
        SimpleXmlUtils.unmarshal(xmlFromServer.getBytes(), Project.ProjectModel.class);
    Assert.assertEquals("test_multi_az_quota_child_1", unmarshal.defaultQuota.getNickname());

    xmlFromServer = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                           + "<Project>\n"
                           + "    <Name>a_test_compute_only_cluster_1</Name>\n"
                           + "    <Type>managed</Type>\n"
                           + "    <Comment></Comment>\n"
                           + "    <State>AVAILABLE</State>\n"
                           + "    <ProjectGroupName></ProjectGroupName>\n"
                           + "    <TenantId></TenantId>\n"
                           + "    <Region>cn-hangzhou</Region>\n"
                           + "    <DefaultQuotaNickname>test_multi_az_quota_child_1</DefaultQuotaNickname> <!-- Deprecated -->\n"
                           + "    <DefaultQuotaRegion>cn-hangzhou</DefaultQuotaRegion>"
                           + "    <DefaultQuotaTenant>tenant_1</DefaultQuotaTenant>"
                           + "    <DefaultCluster>AT-120N</DefaultCluster>\n"
                           + "    <Clusters>\n"
                           + "        <Cluster>\n"
                           + "            <Name>AT-120N</Name>\n"
                           + "            <QuotaID></QuotaID>\n"
                           + "        </Cluster>\n"
                           + "    </Clusters>\n"
                           + "</Project>";
    unmarshal =
        SimpleXmlUtils.unmarshal(xmlFromServer.getBytes(), Project.ProjectModel.class);
    Assert.assertNull(unmarshal.defaultQuota);
  }

}
