package prcessor;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.jetty.util.ajax.JSON;

import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@Tags({ "demo" })
@CapabilityDescription("processor demo file")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor.Builder().name("MY_PROPERTY")
            .displayName("My property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship MY_RELATIONSHIP_SUCCESS = new Relationship.Builder()
            .name("sucess")
            .description("Example relationship Success")
            .build();

    public static final Relationship MY_RELATIONSHIP_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Example relationship Failure")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(MY_PROPERTY);
        descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP_SUCCESS);
        relationships.add(MY_RELATIONSHIP_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // 计划运行时触发
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        ComponentLog log = getLogger();
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final AtomicReference<String> value = new AtomicReference<>();
        log.info("1234444444444 ...........................................................");
        session.read(flowFile, in -> {
            try {
                StringWriter sw = new StringWriter();
                InputStreamReader inr = new InputStreamReader(in);
                char[] buffer = new char[1024];
                int n = 0;
                while (-1 != (n = inr.read(buffer))) {
                    sw.write(buffer, 0, n);
                }
                String str = sw.toString();

                String result = "处理了：" + str + context.getProperty("MY_PROPERTY").getValue();
                final Map<String, String> jsonObj = new HashMap<String, String>();
                jsonObj.put("content", result);
                jsonObj.put("total", "1130");
                String jsonCode = JSON.toString(jsonObj);
                log.info(jsonCode);
                value.set(jsonCode);
            } catch (Exception ex) {
                ex.printStackTrace();
                getLogger().error("Failed to read json string.");
            }
        });

        String results = value.get();

        if (results != null && !results.isEmpty()) {
            flowFile = session.putAttribute(flowFile, "match", results);
        }

        flowFile = session.write(flowFile, out -> out.write(value.get().getBytes()));

        session.transfer(flowFile, MY_RELATIONSHIP_SUCCESS);
    }
}
