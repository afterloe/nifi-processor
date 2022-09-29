package tech.ascs.processors.loop;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.util.ajax.JSON;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ProcessorTesst {

    private TestRunner testRunner;
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    private final AtomicReference<String> value = new AtomicReference<>();

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(Processor.class);
    }

    @Test
    public void testProcessor() {
        final Map<String, String> jsonObj = new HashMap<String, String>();
        jsonObj.put("content", String.format("this is %d value in time", 1));
        jsonObj.put("time", simpleDateFormat.format(new Date()));
        jsonObj.put("total", String.format("%d", jsonObj.get("content").length()));
        String jsonCode = JSON.toString(jsonObj);
        System.out.println(jsonCode);
    }

    @Test
    public void testThread() {
        Thread enginer = new Thread(() -> {
            final Map<String, String> jsonObj = new HashMap<String, String>();
            jsonObj.put("content", String.format("this is %d value in time", 1));
            jsonObj.put("time", simpleDateFormat.format(new Date()));
            jsonObj.put("total", String.format("%d", jsonObj.get("content").length()));
            String jsonCode = JSON.toString(jsonObj);
            value.set(jsonCode);
            System.out.println("fetch cdc msg success -> : " + jsonCode);
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("fetch sleep in thread.");
            }
        });

        enginer.start();
    }

}
