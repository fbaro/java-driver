/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.TypeCodec.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;

/**
 * A registry for {@link TypeCodec}s.
 * <p>
 * {@link CodecRegistry} instances can be created via the {@link #builder()} method:
 * <pre>
 * CodecRegistry registry = CodecRegistry.builder().withCodecs(codec1, codec2).build();
 * </pre>
 * Note that the order in which codecs are added to the registry matters;
 * when looking for a matching codec, the registry will consider all known codecs in
 * the order they have been provided and will return the first matching candidate,
 * even if another codec has a better matching pair of CQL and Java types.
 * <p>
 * To build a {@link CodecRegistry} instance using the default codecs used
 * by the Java driver, simply use the following:
 * <pre>
 * CodecRegistry registry = CodecRegistry.builder().withDefaultCodecs().build();
 * </pre>
 * Note that to be able to deserialize custom CQL types, you could use
 * the following method:
 * <pre>
 * CodecRegistry registry = CodecRegistry.builder().withCustomType(myCustomTypeCodec).build();
 * </pre>
 * {@link CodecRegistry} instances must then be associated with a {@link Cluster} instance:
 * <pre>
 * Cluster cluster = new Cluster.builder().withCodecRegistry(myCodecRegistry).build();
 * </pre>
 * The default {@link CodecRegistry} instance is {@link CodecRegistry#DEFAULT_INSTANCE}.
 * To retrieve the {@link CodecRegistry} instance associated with a Cluster, do the
 * following:
 * <pre>
 * CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
 * </pre>
 *
 */
@SuppressWarnings("unchecked")
public class CodecRegistry {

    private static final Logger logger = LoggerFactory.getLogger(CodecRegistry.class);

    private static final ImmutableSet<TypeCodec<?>> PRIMITIVE_CODECS = ImmutableSet.of(
        BlobCodec.instance,
        BooleanCodec.instance,
        IntCodec.instance,
        BigintCodec.instance,
        CounterCodec.instance,
        DoubleCodec.instance,
        FloatCodec.instance,
        VarintCodec.instance,
        DecimalCodec.instance,
        VarcharCodec.instance,
        AsciiCodec.instance,
        TimestampCodec.instance,
        UUIDCodec.instance,
        TimeUUIDCodec.instance,
        InetCodec.instance
    );

    /**
     * The set of all codecs to handle
     * the default mappings for
     * - Native types
     * - Lists of native types
     * - Sets of native types
     * - Maps of native types
     *
     * Note that there is no support for custom CQL types
     * in the default set of codecs.
     * Codecs for custom CQL types MUST be
     * registered manually.
     */
    private static final ImmutableList<TypeCodec<?>> DEFAULT_CODECS;

    static {
        ImmutableList.Builder<TypeCodec<?>> builder = new ImmutableList.Builder<TypeCodec<?>>();
        builder.addAll(PRIMITIVE_CODECS);
        for (TypeCodec<?> primitiveCodec1 : PRIMITIVE_CODECS) {
            builder.add(new ListCodec(primitiveCodec1));
            builder.add(new SetCodec(primitiveCodec1));
            for (TypeCodec<?> primitiveCodec2 : PRIMITIVE_CODECS) {
                builder.add(new MapCodec(primitiveCodec1, primitiveCodec2));
            }
        }
        DEFAULT_CODECS = builder.build();
    }

    public static final CodecRegistry DEFAULT_INSTANCE = new CodecRegistry(DEFAULT_CODECS, ImmutableMap.<OverrideKey, TypeCodec<?>>of());

    public static class Builder {

        private ImmutableList.Builder<TypeCodec<?>> builder = ImmutableList.builder();

        private ImmutableMap.Builder<OverrideKey, TypeCodec<?>> overrides = ImmutableMap.builder();

        /**
         * Add all 238 default TypeCodecs to the registry.
         *
         * @return this
         */
        public Builder withDefaultCodecs() {
            builder.addAll(DEFAULT_CODECS);
            return this;
        }

        /**
         * TypeCodec instances that have a Java type already
         * present in the set of codec will override
         * the previous one.
         *
         * @param codec The codec to add to the registry
         * @return this
         */
        public Builder withCodec(TypeCodec<?> codec) {
            return withCodec(false, codec);
        }

        public Builder withCodec(boolean includeCollectionCodecs, TypeCodec<?> codec) {
            return withCodecs(includeCollectionCodecs, codec);
        }

        /**
         * TypeCodec instances that have a Java type already
         * present in the set of codec will override
         * the previous one.
         *
         * @param codecs The codecs to add to the registry
         * @return this
         */
        public Builder withCodecs(TypeCodec<?>... codecs) {
            return withCodecs(false, codecs);
        }

        public Builder withCodecs(boolean includeCollectionCodecs, TypeCodec<?>... codecs) {
            for (TypeCodec<?> codec : codecs) {
                if(includeCollectionCodecs) addCodecs(codec);
                else builder.add(codec);
            }
            return this;
        }

        /**
         * Adds required codecs to handle the given custom CQL type.
         *
         * @param customType The custom CQL type to add to the registry
         * @return this
         */
        public Builder withCustomType(DataType customType) {
            addCodecs(new CustomCodec(customType));
            return this;
        }

        private <T> void addCodecs(TypeCodec<T> customCodec) {
            builder.add(customCodec);
            builder.add(new ListCodec<T>(customCodec));
            builder.add(new SetCodec<T>(customCodec));
            for (TypeCodec<?> primitiveCodec : PRIMITIVE_CODECS) {
                builder.add(new MapCodec(primitiveCodec, customCodec));
                builder.add(new MapCodec(customCodec, primitiveCodec));
            }
        }

        public Builder withOverridingCodec(String keyspace, String table, String column, TypeCodec<?> codec) {
            checkNotNull(keyspace, "keyspace cannot be null");
            checkNotNull(table, "table cannot be null");
            checkNotNull(column, "column cannot be null");
            overrides.put(new OverrideKey(keyspace, table, column), codec);
            return this;
        }

        public CodecRegistry build() {
            // reverse to make the last codecs override the first ones
            // if their scope overlap
            return new CodecRegistry(builder.build(), overrides.build());
        }
    }

    /**
     * Create a new instance of {@link com.datastax.driver.core.CodecRegistry.Builder}
     * @return an instance of {@link com.datastax.driver.core.CodecRegistry.Builder}
     * to build {@link CodecRegistry} instances.
     */
    public static CodecRegistry.Builder builder() {
        return new Builder();
    }

    private static final class OverrideKey {

        private final String keysapce;

        private final String table;

        private final String column;

        public OverrideKey(String keysapce, String table, String column) {
            this.keysapce = keysapce;
            this.table = table;
            this.column = column;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            OverrideKey override = (OverrideKey)o;
            return Objects.equal(keysapce, override.keysapce) &&
                Objects.equal(table, override.table) &&
                Objects.equal(column, override.column);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(keysapce, table, column);
        }
    }

    private static final class CacheKey {

        private final TypeToken<?> javaType;

        private final DataType cqlType;

        public CacheKey(TypeToken<?> javaType, DataType cqlType) {
            this.javaType = javaType;
            this.cqlType = cqlType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CacheKey cacheKey = (CacheKey)o;
            return Objects.equal(javaType, cacheKey.javaType) && Objects.equal(cqlType, cacheKey.cqlType);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(javaType, cqlType);
        }
    }

    private final ImmutableList<TypeCodec<?>> codecs;

    private final ImmutableMap<OverrideKey, TypeCodec<?>> overrides;

    private final LoadingCache<CacheKey, TypeCodec<?>> cache;

    private CodecRegistry(ImmutableList<TypeCodec<?>> codecs, ImmutableMap<OverrideKey, TypeCodec<?>> overrides) {
        this.codecs = codecs;
        this.overrides = overrides;
        this.cache = CacheBuilder.newBuilder()
            .initialCapacity(400)
            .build(
                new CacheLoader<CacheKey, TypeCodec<?>>() {
                    public TypeCodec<?> load(CacheKey cacheKey) {
                        return findCodec(cacheKey.cqlType, cacheKey.javaType);
                    }
                });
    }

    /**
     * Called
     * - When binding collection values, and the Java type of the collection element is not known (e.g. GettableData#getObject)
     * BoundStatement setList
     * - When the Java type is irrelevant, such as in toString() methods
     *
     * ArrayBackedRow.toString()  (com.datastax.driver.core)
     *QueryLogger.parameterValueAsString(Definition, ByteBuffer)  (com.datastax.driver.core)
     BuiltStatement.maybeAddRoutingKey(String, Object)  (com.datastax.driver.core.querybuilder)
     Rows in Result in Responses.toString()  (com.datastax.driver.core)
     BoundStatement.setSet(int, Set<T>)  (com.datastax.driver.core)
     BoundStatement.setMap(int, Map<K, V>)(2 usages)  (com.datastax.driver.core)
     BoundStatement.setList(int, List<T>)  (com.datastax.driver.core)

     * @param cqlType The CQL type the codec should deserialize from and serialize to
     * @param <T> The Java type the codec can serialize from and deserialize to
     * @return A suitable codec
     * @throws CodecNotFoundException if a suitable codec cannot be found
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType) throws CodecNotFoundException {
        return lookupCache(cqlType, null);
    }

    /**
     * Called from GettableData instances when
     * calling methods getX, where both CQL type and Java type
     * are imposed by the method contract.
     * <p>
     * One out of two arguments can be {@code null}, but not both.
     * When one argument is {@code null}, it is assumed that its meaning is "ANY",
     * e.g. <code>codecFor(null, String.class)</code> would
     * return the first codec that deserializes from any CQL type
     * to a Java String.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec's Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even
     * if another codec as a better or exact match for it</em>.
     *
     * @param cqlType The CQL type the codec should deserialize from and serialize to
     * @param javaType The Java class the codec can serialize from and deserialize to
     * @param <T> The Java type the codec can serialize from and deserialize to
     * @return A suitable codec
     * @throws CodecNotFoundException if a suitable codec cannot be found
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType) throws CodecNotFoundException {
        return codecFor(cqlType, TypeToken.of(javaType));
    }

    /**
     * Called from GettableData instances when
     * calling methods getX, where both CQL type and Java type
     * are imposed by the method contract, and the Java
     * type is a parameterized type.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec's Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even
     * if another codec as a better or exact match for it</em>.
     *
     * @param cqlType The CQL type the codec should deserialize from and serialize to. Cannot be {@code null}.
     * @param javaType The {@link TypeToken} encapsulating the Java type the codec can serialize from and deserialize to.
     *                 Cannot be {@code null}.
     * @param <T> The Java type the codec can serialize from and deserialize to.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, TypeToken<T> javaType) throws CodecNotFoundException {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(javaType, "Parameter javaType cannot be null");
        return lookupCache(cqlType, javaType);
    }

    /**
     *
     * @param cqlType
     * @param value
     * @param <T>
     * @return
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, T value) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(value, "Parameter value cannot be null");
        return findCodec(cqlType, value);
    }

    /**
     * Called when binding a value whose CQL type and Java type are both unknown.
     * Happens specially when creating SimpleStatement instances, and in the QueryBuilder.
     * CodecUtils.convert
     * Utils.appendValue
     * <p>
     * Note that, due to type erasure, this method works on a best-effort basis
     * and might not return the most appropriate codec,
     * specially for generic types such as collections.
     *
     * @param value The value we are looking a codec for; must not be {@code null}.
     * @param <T> The Java type the codec can serialize from and deserialize to.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(T value) {
        checkNotNull(value, "Parameter value cannot be null");
        return findCodec(null, value);
    }

    // Methods accepting codec overrides on a per-column basis

    public <T> TypeCodec<T> codecFor(DataType cqlType, String keyspace, String table, String column) throws CodecNotFoundException {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        TypeCodec<T> codec = findOverridingCodec(cqlType, null, keyspace, table, column);
        if (codec != null)
            return codec;
        return codecFor(cqlType);
    }

    /**
     * Called
     * - When binding collection values, and the Java type of the collection element is not known (e.g. GettableData#getObject)
     * - When the Java type is irrelevant, such as in toString() methods
     *
     * @param cqlType The CQL type the codec should deserialize from and serialize to
     * @param javaType
     * @param <T> The Java type the codec can serialize from and deserialize to
     * @return A suitable codec
     * @throws CodecNotFoundException if a suitable codec cannot be found
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType, String keyspace, String table, String column) throws CodecNotFoundException {
        return codecFor(cqlType, TypeToken.of(javaType), keyspace, table, column);
    }

    /**
     *
     * @param cqlType
     * @param javaType
     * @param keyspace
     * @param table
     * @param column
     * @param <T>
     * @return
     * @throws CodecNotFoundException
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, TypeToken<T> javaType, String keyspace, String table, String column) throws CodecNotFoundException {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(javaType, "Parameter javaType cannot be null");
        TypeCodec<T> codec = findOverridingCodec(cqlType, javaType, keyspace, table, column);
        if (codec != null)
            return codec;
        return codecFor(cqlType, javaType);
    }

    /**
     * runtime
     * bind methods
     * @param cqlType
     * @param value
     * @param keyspace
     * @param table
     * @param column
     * @param <T>
     * @return
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, T value, String keyspace, String table, String column) {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        checkNotNull(value, "Parameter value cannot be null");
        TypeCodec<T> codec = findOverridingCodec(cqlType, value, keyspace, table, column);
        if (codec != null)
            return codec;
        return codecFor(cqlType, value);
    }

    private <T> TypeCodec<T> lookupCache(DataType cqlType, TypeToken<T> javaType) {
        logger.debug("Looking up overriding codec for {} <-> {}", cqlType, javaType);
        CacheKey cacheKey = new CacheKey(javaType, cqlType);
        try {
            return (TypeCodec<T>)cache.getUnchecked(cacheKey);
        } catch (UncheckedExecutionException e) {
            throw (CodecNotFoundException)e.getCause();
        }
    }

    private <T> TypeCodec<T> findCodec(DataType cqlType, TypeToken<T> javaType) {
        logger.debug("Looking for codec {} <-> {}", cqlType, javaType);
        for (TypeCodec<?> codec : codecs) {
            if ((cqlType == null || codec.accepts(cqlType)) && (javaType == null || codec.accepts(javaType))) {
                logger.debug("Codec found for {} <-> {}: {}", cqlType, javaType, codec.getClass());
                return (TypeCodec<T>)codec;
            }
        }
        return logErrorAndThrow(cqlType, javaType);
    }

    private <T> TypeCodec<T> findCodec(DataType cqlType, T value) {
        TypeToken<?> javaType = TypeToken.of(value.getClass());
        logger.debug("Looking for codec {} <-> {}", cqlType, javaType);
        for (TypeCodec<?> codec : codecs) {
            if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value)) {
                logger.debug("Codec found for {} <-> {}: {}", cqlType, javaType, codec.getClass());
                return (TypeCodec<T>)codec;
            }
        }
        return logErrorAndThrow(cqlType, javaType);
    }

    private <T> TypeCodec<T> findOverridingCodec(DataType cqlType, TypeToken<T> javaType, String keyspace, String table, String column) {
        if (keyspace != null && table != null && column != null) {
            logger.debug("Looking up overriding codec for {}.{}.{}", keyspace, table, column);
            OverrideKey overrideKey = new OverrideKey(keyspace, table, column);
            if (overrides.containsKey(overrideKey)) {
                TypeCodec<?> codec = overrides.get(overrideKey);
                if ((cqlType == null || codec.accepts(cqlType)) && (javaType == null || codec.accepts(javaType))) {
                    logger.debug("Codec found for {}.{}.{}: {}", keyspace, table, column, codec);
                    return (TypeCodec<T>)codec;
                } else {
                    return logErrorAndThrow(cqlType, javaType, keyspace, table, column);
                }
            }
        }
        return null;
    }

    private <T> TypeCodec<T> findOverridingCodec(DataType cqlType, T value, String keyspace, String table, String column) {
        if (keyspace != null && table != null && column != null) {
            logger.debug("Looking up overriding codec for {}.{}.{}", keyspace, table, column);
            OverrideKey overrideKey = new OverrideKey(keyspace, table, column);
            if (overrides.containsKey(overrideKey)) {
                TypeCodec<?> codec = overrides.get(overrideKey);
                if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value)) { // value cannot be null
                    logger.debug("Codec found for {}.{}.{}: {}", keyspace, table, column, codec);
                    return (TypeCodec<T>)codec;
                } else {
                    String msg = String.format("Found overriding codec for %s.%s.%s but it does not accept required pair: CQL type %s <-> Java type %s",
                        keyspace, table, column, "ANY", value.getClass());
                    logger.error(msg);
                    throw new CodecNotFoundException(msg, null, TypeToken.of(value.getClass()));
                }
            }
        }
        return null;
    }

    private <T> TypeCodec<T> logErrorAndThrow(DataType cqlType, TypeToken<?> javaType) {
        String msg = String.format("Codec not found for required pair: CQL type %s <-> Java type %s",
            cqlType == null ? "ANY" : cqlType,
            javaType == null ? "ANY" : javaType);
        logger.error(msg);
        throw new CodecNotFoundException(msg, cqlType, javaType);
    }

    private <T> TypeCodec<T> logErrorAndThrow(DataType cqlType, TypeToken<T> javaType, String keyspace, String table, String column) {
        String msg = String.format("Found overriding codec for %s.%s.%s but it does not accept required pair: CQL type %s <-> Java type %s",
            keyspace, table, column,
            cqlType == null ? "ANY" : cqlType,
            javaType == null ? "ANY" : javaType);
        logger.error(msg);
        throw new CodecNotFoundException(msg, cqlType, javaType);
    }

}
