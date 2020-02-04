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

import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class SessionCredentialsTest {

    @Test
    public void equalsTest(){
        SessionCredentials sessionCredentials=new SessionCredentials("RocketMQ","12345678");
        sessionCredentials.setSecurityToken("abcd");
        SessionCredentials other=new SessionCredentials("RocketMQ","12345678","abcd");
        Assert.assertTrue(sessionCredentials.equals(other));
    }

    @Test
    public void updateContentTest(){
        SessionCredentials sessionCredentials=new SessionCredentials();
        Properties properties=new Properties();
        properties.setProperty(SessionCredentials.ACCESS_KEY,"RocketMQ");
        properties.setProperty(SessionCredentials.SECRET_KEY,"12345678");
        properties.setProperty(SessionCredentials.SECURITY_TOKEN,"abcd");
        sessionCredentials.updateContent(properties);
    }

    @Test
    public void SessionCredentialHashCodeTest(){
        SessionCredentials sessionCredentials=new SessionCredentials();
        Properties properties=new Properties();
        properties.setProperty(SessionCredentials.ACCESS_KEY,"RocketMQ");
        properties.setProperty(SessionCredentials.SECRET_KEY,"12345678");
        properties.setProperty(SessionCredentials.SECURITY_TOKEN,"abcd");
        sessionCredentials.updateContent(properties);
        Assert.assertEquals(sessionCredentials.hashCode(),353652211);
    }

    @Test
    public void SessionCredentialEqualsTest(){
        SessionCredentials sessionCredential1 =new SessionCredentials();
        Properties properties1=new Properties();
        properties1.setProperty(SessionCredentials.ACCESS_KEY,"RocketMQ");
        properties1.setProperty(SessionCredentials.SECRET_KEY,"12345678");
        properties1.setProperty(SessionCredentials.SECURITY_TOKEN,"abcd");
        sessionCredential1.updateContent(properties1);

        SessionCredentials sessionCredential2 =new SessionCredentials();
        Properties properties2=new Properties();
        properties2.setProperty(SessionCredentials.ACCESS_KEY,"RocketMQ");
        properties2.setProperty(SessionCredentials.SECRET_KEY,"12345678");
        properties2.setProperty(SessionCredentials.SECURITY_TOKEN,"abcd");
        sessionCredential2.updateContent(properties2);

        Assert.assertTrue(sessionCredential2.equals(sessionCredential1));
        sessionCredential2.setSecretKey("1234567899");
        sessionCredential2.setSignature("1234567899");
        Assert.assertFalse(sessionCredential2.equals(sessionCredential1));
    }

    @Test
    public void SessionCredentialToStringTest(){
        SessionCredentials sessionCredential1 =new SessionCredentials();
        Properties properties1=new Properties();
        properties1.setProperty(SessionCredentials.ACCESS_KEY,"RocketMQ");
        properties1.setProperty(SessionCredentials.SECRET_KEY,"12345678");
        properties1.setProperty(SessionCredentials.SECURITY_TOKEN,"abcd");
        sessionCredential1.updateContent(properties1);

        Assert.assertEquals(sessionCredential1.toString(),
            "SessionCredentials [accessKey=RocketMQ, secretKey=12345678, signature=null, SecurityToken=abcd]");
    }


}
