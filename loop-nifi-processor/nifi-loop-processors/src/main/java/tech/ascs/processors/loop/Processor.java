package tech.ascs.processors.loop;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.eclipse.jetty.util.ajax.JSON;

import io.debezium.engine.format.Json;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Tags({ "cdc", "loop" })
@CapabilityDescription("postgresql cdc linked processor")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class Processor extends AbstractProcessor {

        private final AtomicReference<String> value = new AtomicReference<>();
        private final AtomicBoolean initEngine = new AtomicBoolean(false);
        private List<PropertyDescriptor> descriptors;
        private Set<Relationship> relationships;

        public static final Relationship FETCH_CDC_SUCCESS = new Relationship.Builder()
                        .name("FETCH_CDC_SUCCESS")
                        .description("success fetch cdc info")
                        .build();

        @Override
        protected void init(final ProcessorInitializationContext context) {
                this.descriptors = PropsTools.initProps();
                this.descriptors = Collections.unmodifiableList(descriptors);

                this.relationships = new HashSet<>();
                this.relationships.add(FETCH_CDC_SUCCESS);
                this.relationships = Collections.unmodifiableSet(relationships);
        }

        @Override
        public Set<Relationship> getRelationships() {
                return this.relationships;
        }

        @Override
        public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return this.descriptors;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) {
                ComponentLog log = getLogger();
                if (!initEngine.get()) {
                        setUpEngine(context, session);
                } else {
                        String results = value.get();
                        if (results == null || results.isEmpty()) {
                                context.yield();
                                return;
                        }
                        FlowFile flowFile = session.create();
                        if (flowFile == null) {
                                initEngine.set(false);
                                log.error("create flowFile fail.");
                                return;
                        }
                        flowFile = session.putAttribute(flowFile, "match", results);

                        flowFile = session.write(flowFile, out -> {
                                byte[] content = value.get().getBytes();
                                out.write(content);
                                log.info(String.format("wrtie %d bytes to flowFile", content.length));
                        });
                        value.set("");
                        session.transfer(flowFile, FETCH_CDC_SUCCESS);
                        session.commit();
                }
        }

        private void setUpEngine(final ProcessContext context, final ProcessSession session) {
                initEngine.set(true);
                ComponentLog log = getLogger();
                log.info("init loop engine ... ...");

                DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                                .using(this.getEngineProperties(context)).notifying((eventList, committer) -> {
                                        String jsonStr = JSON.toString(eventList);
                                        value.set(jsonStr);
                                        log.info("fetch cdc change event: " + jsonStr);
                                }).build();

                ExecutorService executor = Executors.newSingleThreadExecutor();
                executor.execute(engine);

                log.info("cdc engine init done.");
        }

        private Properties getEngineProperties(ProcessContext context) {
                Properties props = new Properties();
                props.setProperty("name", "pg.engine");
                props.setProperty("connector.class",
                                "io.debezium.connector.postgresql.PostgresConnector");
                props.setProperty("offset.storage",
                                "org.apache.kafka.connect.storage.FileOffsetBackingStore");
                props.setProperty("offset.storage.file.filename", "./offsets.dat");
                props.setProperty("offset.flush.interval.ms",
                                context.getProperty("CDC_INTERVAL_MS").getValue());

                props.setProperty("database.hostname",
                                context.getProperty("PG_HOST").getValue());
                props.setProperty("database.port",
                                context.getProperty("PG_PORT").getValue());
                props.setProperty("database.user",
                                context.getProperty("PG_UNAME").getValue());
                props.setProperty("database.password",
                                context.getProperty("PG_PWD").getValue());
                props.setProperty("database.dbname",
                                context.getProperty("PG_DATABASES_NAME").getValue());
                props.setProperty("database.server.name", "fulfillment");
                props.setProperty("slot.name",
                                context.getProperty("CDC_SLOT_NAME").getValue());
                props.setProperty("plugin.name", "pgoutput");

                return props;
        }

        @OnStopped
        public void onStopped(final ProcessContext context) {
                ComponentLog log = getLogger();
                if (initEngine.get()) {
                        log.info("关闭数据流");
                }

        }

        @OnShutdown
        protected void OnShutdown(ProcessContext context) {
                ComponentLog log = getLogger();
                log.info("服务关机");
        }
}
