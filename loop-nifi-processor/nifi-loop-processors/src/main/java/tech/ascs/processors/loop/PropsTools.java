package tech.ascs.processors.loop;

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

public class PropsTools {
    public static List<PropertyDescriptor> initProps() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();

        PropertyDescriptor PG_HOST = new PropertyDescriptor.Builder().name("PG_HOST")
                .displayName("postgres host")
                .description("enter postgres host to link")
                .defaultValue("172.18.10.10")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        PropertyDescriptor PG_UNAME = new PropertyDescriptor.Builder().name("PG_UNAME")
                .displayName("postgres usename")
                .description("enter postgres link username")
                .defaultValue("postgres")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        PropertyDescriptor PG_PWD = new PropertyDescriptor.Builder().name("PG_PWD")
                .displayName("postgres passwrod")
                .description("enter postgres password for user to link")
                .defaultValue("ascs.tech")
                .required(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();

        PropertyDescriptor PG_PORT = new PropertyDescriptor.Builder().name("PG_PORT")
                .displayName("postgres port")
                .description("enter postgres port to link")
                .required(false)
                .addValidator(StandardValidators.INTEGER_VALIDATOR)
                .defaultValue("5432")
                .build();

        PropertyDescriptor PG_DATABASES_NAME = new PropertyDescriptor.Builder()
                .name("PG_DATABASES_NAME")
                .displayName("postgres databases name")
                .description("enter database name to link")
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .required(false)
                .defaultValue("postgres")
                .build();

        PropertyDescriptor CDC_SLOT_NAME = new PropertyDescriptor.Builder().name("CDC_SLOT_NAME")
                .displayName("cdc slot name")
                .description("enter slot cdc name to postgres collections")
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .required(false)
                .defaultValue("pg_cdc_nifi_processor")
                .build();

        PropertyDescriptor CDC_INTERVAL_MS = new PropertyDescriptor.Builder()
                .name("CDC_INTERVAL_MS")
                .displayName("cdc flush interval ms")
                .description("enter ms to flush source")
                .required(false)
                .addValidator(StandardValidators.INTEGER_VALIDATOR)
                .defaultValue("10000")
                .build();

        descriptors.add(PG_HOST);
        descriptors.add(PG_UNAME);
        descriptors.add(PG_PWD);
        descriptors.add(PG_PORT);
        descriptors.add(PG_DATABASES_NAME);
        descriptors.add(CDC_SLOT_NAME);
        descriptors.add(CDC_INTERVAL_MS);

        return descriptors;
    }
}
