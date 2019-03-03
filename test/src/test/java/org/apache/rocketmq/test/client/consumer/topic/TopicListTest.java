package org.apache.rocketmq.test.client.consumer.topic;

import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.test.util.RandomUtils;
import org.junit.Test;

import java.util.Set;

public class TopicListTest {

    public static boolean checkEqual(Set<String> lh, Set<String> rh) {
        if (lh.size() != rh.size()) {
            return false;
        }
        if (!rh.containsAll(lh)) {
            return false;
        }
        return true;
    }

    @Test
    public void topicTestFromJson() {
        TopicList topicList = new TopicList();
        topicList.setBrokerAddr(RandomUtils.getStringByUUID());
        int num= RandomUtils.getIntegerBetween(0,20);
        for (int i = 0; i < num; i++) {
            topicList.getTopicList().add(RandomUtils.getStringByUUID());
        }
        String json = RemotingSerializable.toJson(topicList, true);
        TopicList fromJson = RemotingSerializable.fromJson(json, TopicList.class);
        assert (checkEqual(topicList.getTopicList(), fromJson.getTopicList()));
        assert (fromJson.getBrokerAddr().equals(topicList.getBrokerAddr()));
    }
}
