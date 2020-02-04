/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.acl.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.acl.plain.PlainAccessResource;
import org.junit.Assert;
import org.junit.Test;

public class PermissionTest {

    @Test
    public void fromStringGetPermissionTest() {
        byte perm = Permission.parsePermFromString("PUB");
        Assert.assertEquals(perm, Permission.PUB);

        perm = Permission.parsePermFromString("SUB");
        Assert.assertEquals(perm, Permission.SUB);

        perm = Permission.parsePermFromString("PUB|SUB");
        Assert.assertEquals(perm, Permission.PUB|Permission.SUB);

        perm = Permission.parsePermFromString("SUB|PUB");
        Assert.assertEquals(perm, Permission.PUB|Permission.SUB);

        perm = Permission.parsePermFromString("DENY");
        Assert.assertEquals(perm, Permission.DENY);

        perm = Permission.parsePermFromString("1");
        Assert.assertEquals(perm, Permission.DENY);

        perm = Permission.parsePermFromString(null);
        Assert.assertEquals(perm, Permission.DENY);

    }

    @Test
    public void checkPermissionTest() {
        boolean boo = Permission.checkPermission(Permission.DENY, Permission.DENY);
        Assert.assertFalse(boo);

        boo = Permission.checkPermission(Permission.PUB, Permission.PUB);
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.SUB, Permission.SUB);
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.PUB, (byte) (Permission.PUB|Permission.SUB));
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.SUB, (byte) (Permission.PUB|Permission.SUB));
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.ANY, (byte) (Permission.PUB|Permission.SUB));
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.ANY, Permission.SUB);
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.ANY, Permission.PUB);
        Assert.assertTrue(boo);

        boo = Permission.checkPermission(Permission.DENY, Permission.ANY);
        Assert.assertFalse(boo);

        boo = Permission.checkPermission(Permission.DENY, Permission.PUB);
        Assert.assertFalse(boo);

        boo = Permission.checkPermission(Permission.DENY, Permission.SUB);
        Assert.assertFalse(boo);

    }

    @Test(expected = AclException.class)
    public void setTopicPermTest() {
        PlainAccessResource plainAccessResource = new PlainAccessResource();
        Map<String, Byte> resourcePermMap = plainAccessResource.getResourcePermMap();

        Permission.parseResourcePerms(plainAccessResource, false, null);
        Assert.assertNull(resourcePermMap);

        List<String> groups = new ArrayList<>();
        Permission.parseResourcePerms(plainAccessResource, false, groups);
        Assert.assertNull(resourcePermMap);

        groups.add("groupA=DENY");
        groups.add("groupB=PUB|SUB");
        groups.add("groupC=PUB");
        Permission.parseResourcePerms(plainAccessResource, false, groups);
        resourcePermMap = plainAccessResource.getResourcePermMap();

        byte perm = resourcePermMap.get(PlainAccessResource.getRetryTopic("groupA"));
        Assert.assertEquals(perm, Permission.DENY);

        perm = resourcePermMap.get(PlainAccessResource.getRetryTopic("groupB"));
        Assert.assertEquals(perm,Permission.PUB|Permission.SUB);

        perm = resourcePermMap.get(PlainAccessResource.getRetryTopic("groupC"));
        Assert.assertEquals(perm, Permission.PUB);

        List<String> topics = new ArrayList<>();
        topics.add("topicA=DENY");
        topics.add("topicB=PUB|SUB");
        topics.add("topicC=PUB");

        Permission.parseResourcePerms(plainAccessResource, true, topics);

        perm = resourcePermMap.get("topicA");
        Assert.assertEquals(perm, Permission.DENY);

        perm = resourcePermMap.get("topicB");
        Assert.assertEquals(perm, Permission.PUB|Permission.SUB);

        perm = resourcePermMap.get("topicC");
        Assert.assertEquals(perm, Permission.PUB);

        List<String> erron = new ArrayList<>();
        erron.add("");
        Permission.parseResourcePerms(plainAccessResource, false, erron);
    }

    @Test
    public void checkAdminCodeTest() {
        Set<Integer> code = new HashSet<>();
        code.add(17);
        code.add(25);
        code.add(215);
        code.add(200);
        code.add(207);

        for (int i = 0; i < 400; i++) {
            boolean boo = Permission.needAdminPerm(i);
            if (boo) {
                Assert.assertTrue(code.contains(i));
            }
        }
    }

    @Test
    public void AclExceptionTest(){
        AclException aclException = new AclException("CAL_SIGNATURE_FAILED",10015);
        AclException aclExceptionWithMessage = new AclException("CAL_SIGNATURE_FAILED",10015,"CAL_SIGNATURE_FAILED Exception");
        Assert.assertEquals(aclException.getCode(),10015);
        Assert.assertEquals(aclExceptionWithMessage.getStatus(),"CAL_SIGNATURE_FAILED");
        aclException.setCode(10016);
        Assert.assertEquals(aclException.getCode(),10016);
        aclException.setStatus("netaddress examine scope Exception netaddress");
        Assert.assertEquals(aclException.getStatus(),"netaddress examine scope Exception netaddress");
    }
}
