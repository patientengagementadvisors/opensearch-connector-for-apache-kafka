/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.kafka.connect.opensearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

public class DocumentIDStrategyTest {

    @Test
    void noneTypeAlwaysWithoutKey() {
        final var documentId = DocumentIDStrategy.NONE.documentId(createRecord());
        assertNull(documentId);
    }

    @Test
    void topicPartitionOffsetTypeKey() {
        final var documentId = DocumentIDStrategy.TOPIC_PARTITION_OFFSET.documentId(createRecord());
        assertEquals(String.format("%s+%s+%s", "a", 1, 13), documentId);
    }

    @Test
    void recordKeyTypeKey() {
        final var stringKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY.documentId(createRecord());
        assertEquals("a", stringKeySchemaDocumentId);

        final var int8KeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(Schema.INT8_SCHEMA, (byte) 10));
        assertEquals("10", int8KeySchemaDocumentId);

        final var int8KWithoutKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(null, (byte) 10));
        assertEquals("10", int8KWithoutKeySchemaDocumentId);

        final var int16KeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(Schema.INT8_SCHEMA, (short) 11));
        assertEquals("11", int16KeySchemaDocumentId);

        final var int16WithoutKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(null, (short) 11));
        assertEquals("11", int16WithoutKeySchemaDocumentId);

        final var int32KeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(Schema.INT8_SCHEMA, 12));
        assertEquals("12", int32KeySchemaDocumentId);

        final var int32WithoutKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY.documentId(createRecord(null, 12));
        assertEquals("12", int32WithoutKeySchemaDocumentId);

        final var int64KeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(Schema.INT64_SCHEMA, (long) 13));
        assertEquals("13", int64KeySchemaDocumentId);

        final var int64WithoutKeySchemaDocumentId = DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(null, (long) 13));
        assertEquals("13", int64WithoutKeySchemaDocumentId);
    }

    @Test
    void recordKeyTypeThrowsDataException() {
        // unsupported schema type
        assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(Schema.FLOAT64_SCHEMA, 1f)));
        // unknown type without Key schema
        assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(Schema.FLOAT64_SCHEMA, 1f)));
    }

    @Test
    void recordKeyTypeThrowsDataExceptionForNoKeyValue() {
        assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(Schema.FLOAT64_SCHEMA, null)));
    }

    @Test
    void recordKeyWithMapKeyAndFieldName() {
        final Map<String, Object> mapKey = new HashMap<>();
        mapKey.put("id", "abc123");
        mapKey.put("tenant", "acme");

        final var documentId = DocumentIDStrategy.RECORD_KEY.documentId(createRecord(null, mapKey), Optional.of("id"));
        assertEquals("abc123", documentId);
    }

    @Test
    void recordKeyWithMapKeyThrowsExceptionWhenFieldNameNotProvided() {
        final Map<String, Object> mapKey = new HashMap<>();
        mapKey.put("id", "abc123");

        final var exception = assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(null, mapKey), Optional.empty()));
        assertEquals("Key is a MAP but no field name was specified. Set 'record.key.field' configuration.",
                exception.getMessage());
    }

    @Test
    void recordKeyWithMapKeyThrowsExceptionWhenFieldNotFound() {
        final Map<String, Object> mapKey = new HashMap<>();
        mapKey.put("id", "abc123");

        final var exception = assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(null, mapKey), Optional.of("nonexistent")));
        assertEquals("Field 'nonexistent' not found in key map or is null.", exception.getMessage());
    }

    @Test
    void recordKeyWithMapKeyThrowsExceptionWhenFieldIsNull() {
        final Map<String, Object> mapKey = new HashMap<>();
        mapKey.put("id", null);

        final var exception = assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(null, mapKey), Optional.of("id")));
        assertEquals("Field 'id' not found in key map or is null.", exception.getMessage());
    }

    @Test
    void recordKeyWithStructKeyAndFieldName() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("tenant", Schema.STRING_SCHEMA)
                .build();
        final Struct structKey = new Struct(keySchema).put("id", "xyz789").put("tenant", "acme");

        final var documentId = DocumentIDStrategy.RECORD_KEY.documentId(createRecord(keySchema, structKey),
                Optional.of("id"));
        assertEquals("xyz789", documentId);
    }

    @Test
    void recordKeyWithStructKeyThrowsExceptionWhenFieldNameNotProvided() {
        final Schema keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
        final Struct structKey = new Struct(keySchema).put("id", "xyz789");

        final var exception = assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(keySchema, structKey), Optional.empty()));
        assertEquals("Key is a STRUCT but no field name was specified. Set 'record.key.field' configuration.",
                exception.getMessage());
    }

    @Test
    void recordKeyWithStructKeyThrowsExceptionWhenFieldNotFound() {
        final Schema keySchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
        final Struct structKey = new Struct(keySchema).put("id", "xyz789");

        final var exception = assertThrows(DataException.class, () -> DocumentIDStrategy.RECORD_KEY
                .documentId(createRecord(keySchema, structKey), Optional.of("nonexistent")));
        assertEquals("Field 'nonexistent' not found in key struct schema.", exception.getMessage());
    }

    @Test
    void recordKeyWithStructKeyThrowsExceptionWhenFieldIsNull() {
        final Schema keySchema = SchemaBuilder.struct().field("id", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct structKey = new Struct(keySchema).put("id", null);

        final var exception = assertThrows(DataException.class,
                () -> DocumentIDStrategy.RECORD_KEY.documentId(createRecord(keySchema, structKey), Optional.of("id")));
        assertEquals("Field 'id' in key struct is null.", exception.getMessage());
    }

    @Test
    void recordKeyWithStructKeySupportsIntegerField() {
        final Schema keySchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct structKey = new Struct(keySchema).put("id", 12345).put("name", "test");

        final var documentId = DocumentIDStrategy.RECORD_KEY.documentId(createRecord(keySchema, structKey),
                Optional.of("id"));
        assertEquals("12345", documentId);
    }

    SinkRecord createRecord() {
        return createRecord(Schema.STRING_SCHEMA, "a");
    }

    SinkRecord createRecord(final Schema keySchema, final Object keyValue) {
        return new SinkRecord("a", 1, keySchema, keyValue,
                SchemaBuilder.struct().name("struct").field("string", Schema.STRING_SCHEMA).build(), null, 13);
    }

}
