package org.apache.rocketmq.broker.offset;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerOffsetManagerTest {

    private String groupName = "GroupTest";
    private static final String TOPIC_GROUP_SEPARATOR = "@";
    // GroupTest groupName in consumerOffset.json
    String consumerOffsetTestData = "{\"dataVersion\":{\"counter\":0," +
            "\"timestamp\":1569126408715},\"offsetTable\":{\"TopicTest@please_rename_unique_group_name_4\":{0:250,1:250,2:250,3:250}," +
            "\"%RETRY%please_rename_unique_group_name_4@please_rename_unique_group_name_4\":{0:0}," +
            "\"TopicTest@GroupTest\":{0:250,1:251,2:252,3:253}," +
            "\"%RETRY%TopicTest@GroupTest\":{0:0}}}";

    private ConsumerOffsetManager consumerOffsetManager = new ConsumerOffsetManager(null);

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> offsetTable =
            new ConcurrentHashMap<>( 0x200 );

    private ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    private void testConsumerManagerInit(){
        ConsumerOffsetManagerTest obj = RemotingSerializable.fromJson(consumerOffsetTestData,
                ConsumerOffsetManagerTest.class);
        this.offsetTable = obj.offsetTable;
    }

    @Test
    public void  testCleanConsumerOffsetList(){
        testConsumerManagerInit();
        consumerOffsetManager.cleanConsumerOffsetList(groupName);
        for (Map.Entry<String, ConcurrentMap<Integer, Long>> map : this.offsetTable.entrySet()) {
            int indexAtGroup = map.getKey().lastIndexOf(TOPIC_GROUP_SEPARATOR + groupName);
            if (indexAtGroup != -1) {
                // this topicName Contain system auto create example:%RETRY%+Topic+Group
                String topic = map.getKey().substring(0,indexAtGroup);
                String cleanTopicAtGroup = topic + TOPIC_GROUP_SEPARATOR + groupName;
                ConcurrentMap<Integer, Long> result = this.offsetTable.remove(cleanTopicAtGroup);
                if (result != null) {
                    assertThat(result).isNotNull();
                    String resultString = "{TopicTest@please_rename_unique_group_name_4={0=250, 1=250, 2=250, 3=250}, %RETRY%please_rename_unique_group_name_4@please_rename_unique_group_name_4={0=0}}";
                    assertThat(result.toString()).as(resultString);
                }
            }
        }
    }

    @Test
    public void testQueryOffset(){
        testConsumerManagerInit();
        consumerOffsetManager.setOffsetTable((ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>)getOffsetTable());
        String topicName = "TopicTest";
        long offest0 = consumerOffsetManager.queryOffset(groupName, topicName,0);
        assertThat(offest0).isEqualTo(0xfa);
        long offest1 = consumerOffsetManager.queryOffset(groupName, topicName,1);
        assertThat(offest1).isEqualTo(0xfb);
        long offest2 = consumerOffsetManager.queryOffset(groupName, topicName,2);
        assertThat(offest2).isEqualTo(0xfc);
        long offest3 = consumerOffsetManager.queryOffset(groupName, topicName,3);
        assertThat(offest3).isEqualTo(0xfd);
    }
}