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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.protocol.body.GroupList;
import org.junit.Test;

import java.util.HashSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by guoyao on 2019/2/18.
 */
public class GroupListTest {

    @Test
    public void testSetGet() throws Exception {
        HashSet<String> fisrtUniqueSet=createUniqueNewSet();
        HashSet<String> secondUniqueSet=createUniqueNewSet();
        assertThat(fisrtUniqueSet).isNotEqualTo(secondUniqueSet);
        GroupList gl=new GroupList();
        gl.setGroupList(fisrtUniqueSet);
        assertThat(gl.getGroupList()).isEqualTo(fisrtUniqueSet);
        assertThat(gl.getGroupList()).isNotEqualTo(secondUniqueSet);
        gl.setGroupList(secondUniqueSet);
        assertThat(gl.getGroupList()).isNotEqualTo(fisrtUniqueSet);
        assertThat(gl.getGroupList()).isEqualTo(secondUniqueSet);
    }

    private HashSet<String> createUniqueNewSet() {
        HashSet<String> groups=new HashSet<String>();
        groups.add(UUID.randomUUID().toString());
        return groups;
    }
}
