/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.util.Optional;

public abstract class ErrOrResult<E, R>{

    private ErrOrResult() {
    }

    public static <E, R> ErrOrResult<E, R> error(E value) {
        return new Error(value);
    }

    public static <E, R> ErrOrResult<E, R> result(R value) {
        return new Result(value);
    }

    public abstract boolean hasError();

    public abstract Optional<E> error();

    public abstract Optional<R> result();

    public static final class Error<E, R> extends ErrOrResult<E, R> {

        private final E error;

        private Error(E err) {
            super();
            error = err;
        }

        @Override
        public boolean hasError() {
            return true;
        }

        @Override
        public Optional<E> error() {
            return Optional.of(error);
        }

        @Override
        public Optional<R> result() {
            return Optional.empty();
        }
    }

    public static final class Result<E, R> extends ErrOrResult<E, R> {

        private final R result;

        private Result(R res) {
            super();
            result = res;
        }

        @Override
        public boolean hasError() {
            return false;
        }

        @Override
        public Optional<E> error() {
            return Optional.empty();
        }

        @Override
        public Optional<R> result() {
            return Optional.of(result);
        }
    }
}

