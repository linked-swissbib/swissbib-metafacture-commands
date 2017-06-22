package org.swissbib.linked.mf.source;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.culturegraph.mf.framework.FluxCommand;
import org.culturegraph.mf.framework.ObjectReceiver;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.annotations.In;
import org.culturegraph.mf.framework.annotations.Out;
import org.culturegraph.mf.framework.helpers.DefaultObjectPipe;

import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author Sebastian Schüpbach
 * @version 0.1
 *          <p>
 *          Created on 6/20/17
 */
@Description("Acts as a Kafka consumer and input source for a Metafacture workflow.")
@FluxCommand("read-kafka")
@In(String.class)
@Out(Reader.class)
public class MfKafkaConsumer extends DefaultObjectPipe<String, ObjectReceiver<Reader>> {

    private String server = "localhost:9092";
    private String[] topics;
    private String groupid;

    public void setTopics(String topics) {
        this.topics = topics.split("#");
    }

    public void setGroupId(String groupId) {
        this.groupid = groupId;
    }

    @Override
    public void process(String server) {
        this.server = server;
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(setProps());
        consumer.subscribe(Arrays.asList(topics));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                getReceiver().process(new StringReader(record.value()));
        }

    }

    private Properties setProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", server);
        props.put("group.id", groupid);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

}
