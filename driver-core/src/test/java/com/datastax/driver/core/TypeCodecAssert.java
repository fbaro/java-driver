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

import com.google.common.reflect.TypeToken;
import org.assertj.core.api.AbstractAssert;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 */
public class TypeCodecAssert extends AbstractAssert<TypeCodecAssert, TypeCodec> {

    protected TypeCodecAssert(TypeCodec<?> actual) {
        super(actual, TypeCodecAssert.class);
    }

    public TypeCodecAssert accepts(Class<?> clazz) {
        return accepts(TypeToken.of(clazz));
    }

    public TypeCodecAssert accepts(TypeToken<?> javaType) {
        assertThat(actual.accepts(javaType)).isTrue();
        return this;
    }

    public TypeCodecAssert accepts(Object value) {
        assertThat(actual.accepts(value)).isTrue();
        return this;
    }

    public TypeCodecAssert accepts(DataType cqlType) {
        assertThat(actual.accepts(cqlType)).isTrue();
        return this;
    }

}
