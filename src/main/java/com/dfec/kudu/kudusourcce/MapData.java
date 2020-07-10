package com.dfec.kudu.kudusourcce;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/10
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class MapData implements Map<String, Object>, Serializable, Cloneable {
    private static final long serialVersionUID = 1L;
    private final LinkedHashMap<String, Object> targetMap;
    private final HashMap<String, String> caseInsensitiveKeys;
    private final Locale locale;
    private SimpleDateFormat formatter;

    public MapData() {
        this((Locale)null);
    }

    public MapData(Locale locale) {
        this(16, locale);
    }

    public MapData(int initialCapacity) {
        this(initialCapacity, (Locale)null);
    }

    public MapData(int initialCapacity, Locale locale) {
        this.formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.targetMap = new LinkedHashMap<String, Object>(initialCapacity) {
            public boolean containsKey(Object key) {
                return MapData.this.containsKey(key);
            }

            protected boolean removeEldestEntry(Entry<String, Object> eldest) {
                boolean doRemove = MapData.this.removeEldestEntry(eldest);
                if (doRemove) {
                    MapData.this.caseInsensitiveKeys.remove(MapData.this.convertKey((String)eldest.getKey()));
                }

                return doRemove;
            }
        };
        this.caseInsensitiveKeys = new HashMap(initialCapacity);
        this.locale = locale != null ? locale : Locale.getDefault();
    }

    private MapData(MapData other) {
        this.formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.targetMap = (LinkedHashMap)other.targetMap.clone();
        this.caseInsensitiveKeys = (HashMap)other.caseInsensitiveKeys.clone();
        this.locale = other.locale;
    }

    public int size() {
        return this.targetMap.size();
    }

    public boolean isEmpty() {
        return this.targetMap.isEmpty();
    }

    public boolean containsKey(Object key) {
        return key instanceof String && this.caseInsensitiveKeys.containsKey(this.convertKey((String)key));
    }

    public boolean containsValue(Object value) {
        return this.targetMap.containsValue(value);
    }

    public Object get(Object key) {
        if (key instanceof String) {
            String caseInsensitiveKey = (String)this.caseInsensitiveKeys.get(this.convertKey((String)key));
            if (caseInsensitiveKey != null) {
                return this.targetMap.get(caseInsensitiveKey);
            }
        }

        return null;
    }

    public String getString(Object key) {
        Object v = this.get(key);
        if (v != null) {
            return v instanceof Date ? this.formatter.format((Date)v) : v.toString();
        } else {
            return null;
        }
    }

    public Double getDouble(Object key) {
        Object v = this.get(key);
        if (v != null) {
            try {
                Double a = Double.parseDouble(v.toString());
                return a;
            } catch (Exception var4) {
            }
        }

        return null;
    }

    public Double getDouble(Object key, double defaultValue) {
        Double v = this.getDouble(key);
        return v == null ? defaultValue : v;
    }

    public Integer getInt(Object key) {
        Object v = this.get(key);
        if (v != null) {
            try {
                Integer a = Integer.parseInt(v.toString());
                return a;
            } catch (Exception var4) {
            }
        }

        return null;
    }

    public Integer getInt(Object key, int defaultValue) {
        Integer v = this.getInt(key);
        return v == null ? defaultValue : v;
    }

    public MapData getMapData(Object key) {
        Object v = this.get(key);
        return v != null && v instanceof MapData ? (MapData)v : null;
    }

    public List<MapData> getMapList(Object key) {
        Object v = this.get(key);
        return v != null && v instanceof List ? (List)v : null;
    }

    public Object getOrDefault(Object key, Object defaultValue) {
        if (key instanceof String) {
            String caseInsensitiveKey = (String)this.caseInsensitiveKeys.get(this.convertKey((String)key));
            if (caseInsensitiveKey != null) {
                return this.targetMap.get(caseInsensitiveKey);
            }
        }

        return defaultValue;
    }

    public Object put(String key, Object value) {
        String oldKey = (String)this.caseInsensitiveKeys.put(this.convertKey(key), key);
        if (oldKey != null && !oldKey.equals(key)) {
            this.targetMap.remove(oldKey);
        }

        return this.targetMap.put(key, value);
    }

    public void putAll(Map<? extends String, ? extends Object> map) {
        if (!map.isEmpty()) {
            Iterator var2 = map.entrySet().iterator();

            while(var2.hasNext()) {
                Entry<? extends String, ? extends Object> entry = (Entry)var2.next();
                this.put((String)entry.getKey(), entry.getValue());
            }
        }

    }

    public Object remove(Object key) {
        if (key instanceof String) {
            String caseInsensitiveKey = (String)this.caseInsensitiveKeys.remove(this.convertKey((String)key));
            if (caseInsensitiveKey != null) {
                return this.targetMap.remove(caseInsensitiveKey);
            }
        }

        return null;
    }

    public void clear() {
        this.caseInsensitiveKeys.clear();
        this.targetMap.clear();
    }

    public Set<String> keySet() {
        return this.targetMap.keySet();
    }

    public Collection<Object> values() {
        return this.targetMap.values();
    }

    public Set<Entry<String, Object>> entrySet() {
        return this.targetMap.entrySet();
    }

    public MapData clone() {
        return new MapData(this);
    }

    public boolean equals(Object obj) {
        return this.targetMap.equals(obj);
    }

    public int hashCode() {
        return this.targetMap.hashCode();
    }

    public String toString() {
        return this.targetMap.toString();
    }

    protected String convertKey(String key) {
        return key.toLowerCase(this.locale);
    }

    protected boolean removeEldestEntry(Entry<String, Object> eldest) {
        return false;
    }
}