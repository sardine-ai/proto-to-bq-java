package java.ai.sardine.prototobq;
import com.google.protobuf.*;
import com.google.api.services.bigquery.model.TableRow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

public class ProtoToBigQuery {

    public static class TableRowResult {
        private final TableRow row;
        private final boolean hasUnknownFields;

        public TableRowResult(TableRow row, boolean hasUnknownFields) {
            this.row = row;
            this.hasUnknownFields = hasUnknownFields;
        }

        @Nullable
        public TableRow getRow() {
            return row;
        }

        public boolean isHasUnknownFields() {
            return hasUnknownFields;
        }
    }

    public static TableRowResult toTableRow(GeneratedMessageV3 fromProto) {
        if (fromProto.getAllFields().isEmpty()) {
            return new TableRowResult(null, false);
        }
        TableRow row = new TableRow();
        boolean hasUnknownFields = copyFields(fromProto, row);
        return new TableRowResult(row, hasUnknownFields);
    }

    private static boolean copyFields(GeneratedMessageV3 fromProto, TableRow toRow) {
        // all known fields, including ones that don't have value set
        List<Descriptors.FieldDescriptor> allFields = fromProto.getDescriptorForType().getFields();
        // getAllFields returns all known fields with value set
        Map<Descriptors.FieldDescriptor, Object> fields = fromProto.getAllFields();
        boolean hasUnknownFields = false;
        for (Descriptors.FieldDescriptor field: allFields) {
            Object value = fields.get(field);
            String columnName = field.getName();
            switch (field.getJavaType()) {
                case MESSAGE:
                    if (value == null) {
                        // no-op
                    } else if (field.isRepeated()) {
                        List<?> list = (List<?>) value;
                        List<TableRow> rows = new ArrayList<>();
                        for (Object msg: list) {
                            if (msg instanceof MapEntry) {
                                MapEntry<?, ?> entry = (MapEntry<?, ?>) msg;
                                Object entryKey = entry.getKey();
                                Object entryValue = entry.getValue();
                                if (entryValue instanceof GeneratedMessageV3) {
                                    TableRowResult r = toTableRow((GeneratedMessageV3) entryValue);
                                    rows.add(new TableRow().set("key", entryKey).set("value", r.row));
                                    hasUnknownFields |= r.hasUnknownFields;
                                } else {
                                    rows.add(new TableRow().set("key", entryKey).set("value", entryValue));
                                }
                            } else {
                                TableRowResult r = toTableRow((GeneratedMessageV3) msg);
                                rows.add(r.row);
                                hasUnknownFields |= r.hasUnknownFields;
                            }
                        }
                        toRow.set(columnName, rows);
                    } else {
                        TableRowResult r = toTableRow((GeneratedMessageV3) value);
                        toRow.set(columnName, r.row);
                        hasUnknownFields |= r.hasUnknownFields;
                    }
                    break;
                case STRING:
                case LONG:
                case INT:
                case FLOAT:
                case DOUBLE:
                case BYTE_STRING:
                    if (value != null) {
                        toRow.set(columnName, value);
                    }
                    break;
                case ENUM:
                    if (value != null) {
                        Descriptors.EnumValueDescriptor valueAsEnum = (Descriptors.EnumValueDescriptor) value;
                        if (valueAsEnum.getNumber() != 0) {
                            toRow.set(columnName, valueAsEnum.getName());
                        }
                    }
                    break;
                case BOOLEAN:
                    if (value == null) {
                        // set false as zero-value since it's usually what we want when reading data
                        toRow.set(columnName, false);
                    } else {
                        toRow.set(columnName, value);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unexpected type " + field.getJavaType());
            }
        }

        // getUnknownFields returns all unknown fields
        Map<Integer, UnknownFieldSet.Field> unknownFields = fromProto.getUnknownFields().asMap();
        return hasUnknownFields || unknownFields.size() > 0;
    }
}
