
package org.apache.rocketmq.client.opentracing.hook;

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;

public class SendMessageOpenTracingHookImpl   implements SendMessageHook {
    private Span rootSpan;
    private Span span;
    private Tracer tracer;

    public SendMessageOpenTracingHookImpl(Span rootSpan,Tracer tracer){
        this.rootSpan = rootSpan;
        this.tracer = tracer;

    }

    @Override
    public String hookName() {
        return "SendMessageOpenTracingHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        span = tracer.buildSpan("RocketMQProducer").withTag(Tags.SPAN_KIND.getKey(),Tags.SPAN_KIND_PRODUCER).asChildOf(rootSpan).start();
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        Map<String, String> map = new HashMap();
        map.put("topic",context.getSendResult().getMessageQueue().getTopic());
        map.put("msgId",context.getSendResult().getMsgId());
        map.put("offsetMsgId",context.getSendResult().getOffsetMsgId());
        map.put("tag",context.getMessage().getTags());
        map.put("key",context.getMessage().getKeys());
        map.put("msgType",context.getMsgType().toString());
        map.put("bodyLength",String.valueOf(context.getMessage().getBody().length));
        span.log(map);
        span.finish();
    }
}
