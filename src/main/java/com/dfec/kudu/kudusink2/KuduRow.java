package com.dfec.kudu.kudusink2;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/9
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public class KuduRow extends Row {

    private Map<String, Integer> rowNames;

    public KuduRow(Integer arity) {
        super(arity);
        rowNames = new LinkedHashMap<>();
    }

    public Object getField(String name) {
        return super.getField(rowNames.get(name));
    }

    public void setField(int pos, String name, Object value) {
        super.setField(pos, value);
        this.rowNames.put(name, pos);
    }

    public boolean isNull(String name) {
        return isNull(rowNames.get(name));
    }

    public boolean isNull(int pos) {
        return getField(pos) == null;
    }

    private static int validFields(Object object) {
        Long validField = 0L;
        for (Class<?> c = object.getClass(); c != null; c = c.getSuperclass()) {
            validField += basicValidation(c.getDeclaredFields()).count();
        }
        return validField.intValue();
    }

    private static Stream<Field> basicValidation(Field[] fields) {
        return Arrays.stream(fields)
                .filter(cField -> !Modifier.isStatic(cField.getModifiers()))
                .filter(cField -> !Modifier.isTransient(cField.getModifiers()));
    }

    public Map<String,Object> blindMap() {
        Map<String,Object> toRet = new LinkedHashMap<>();
        rowNames.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .forEach(entry -> toRet.put(entry.getKey(), super.getField(entry.getValue())));
        return  toRet;
    }

    @Override
    public String toString() {
        return blindMap().toString();
    }
}