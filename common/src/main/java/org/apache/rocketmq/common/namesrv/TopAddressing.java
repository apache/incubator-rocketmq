package org.apache.rocketmq.common.namesrv;

import java.util.function.Consumer;

public interface TopAddressing {

    String fetchNSAddr();

    void registerChangeCallBack(Consumer<String> changeCallBack);
}
