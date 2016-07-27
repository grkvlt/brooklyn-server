/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent.Scope;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.apache.brooklyn.util.exceptions.FatalConfigurationRuntimeException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;


/**
 * Utility class to evaluate test-framework assertions
 */
public class TestFrameworkAssertions {

    public enum Condition {
        IS_NULL {
            @Override
            public boolean check(Object actual, Object expected) {
                return isTrue(expected) == (actual == null);
            }
        },
        NOT_NULL {
            @Override
            public boolean check(Object actual, Object expected) {
                return isTrue(expected) == (actual != null);
            }
        },
        IS_EQUAL_TO {
            @Override
            public boolean check(Object actual, Object expected) {
                return (actual != null) && actual.equals(expected);
            }
        },
        EQUAL_TO {
            @Override
            public boolean check(Object actual, Object expected) {
                return (actual != null) && actual.equals(expected);
            }
        },
        EQUALS {
            @Override
            public boolean check(Object actual, Object expected) {
                return (actual != null) && actual.equals(expected);
            }
        },
        MATCHES {
            @Override
            public boolean check(Object actual, Object expected) {
                return (actual != null) && actual.toString().matches(expected.toString());
            }
        },
        CONTAINS {
            @Override
            public boolean check(Object actual, Object expected) {
                if (actual instanceof Iterable) {
                    return (actual != null) && Iterables.contains((Iterable<?>) actual, expected);
                } else {
                    return (actual != null) && actual.toString().contains(expected.toString());
                }
            }
        },
        IS_EMPTY {
            @Override
            public boolean check(Object actual, Object expected) {
                if (actual instanceof Iterable) {
                    return isTrue(expected) == (actual != null && Iterables.isEmpty((Iterable<?>) actual));
                } else {
                    return isTrue(expected) == (actual != null && Strings.isEmpty(actual.toString()));
                }
            }
        },
        NOT_EMPTY {
            @Override
            public boolean check(Object actual, Object expected) {
                if (actual instanceof Iterable) {
                    return isTrue(expected) == (actual != null && Iterables.size((Iterable<?>) actual) > 0);
                } else {
                    return isTrue(expected) == (actual != null && Strings.isNonEmpty(actual.toString()));
                }
            }
        },
        HAS_TRUTH_VALUE {
            @Override
            public boolean check(Object actual, Object expected) {
                return isTrue(expected) == isTrue(actual);
            }
        };

        public static Condition UNKNOWN = null;

        private static Converter<String, String> converter = CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.UPPER_UNDERSCORE);

        public static Condition fromString(String name) {
            Maybe<Condition> parsed = tryFromString(name);
            return parsed.get();
        }

        public static Maybe<Condition> tryFromString(String name) {
            try {
                Condition scope = valueOf(converter.convert(name));
                return Maybe.of(scope);
            } catch (Exception cause) {
                return Maybe.absent(cause);
            }
        }

        public static boolean isValid(String name) {
            Maybe<Condition> check = tryFromString(name);
            return check.isPresentAndNonNull();
        }

        @Override
        public String toString() {
            return converter.reverse().convert(name());
        }

        public abstract boolean check(Object actual, Object expected);

    }

    private TestFrameworkAssertions() { }

    /**
     * Get assertions tolerantly from a configuration key.
     * This supports either a simple map of assertions, such as
     * <pre>
     * assertOut:
     *   contains: 2 users
     *   matches: .*[\d]* days.*
     * </pre>
     * or a list of such maps, (which allows you to repeat keys):
     * <pre>
     * assertOut:
     * - contains: 2 users
     * - contains: 2 days
     * </pre>
     * or
     * <pre>
     * private static List<Map<String,Object>> getAssertions(ConfigKey<Object> key) {
     * }
     * </pre>
     */
    public static List<Map<String, Object>> getAssertions(Entity entity, ConfigKey<Object> key) {
        Object config = entity.getConfig(key);
        Maybe<Map<String, Object>> maybeMap = TypeCoercions.tryCoerce(config, new TypeToken<Map<String, Object>>() {});
        if (maybeMap.isPresent()) {
            return Collections.singletonList(maybeMap.get());
        }

        Maybe<List<Map<String, Object>>> maybeList = TypeCoercions.tryCoerce(config,
            new TypeToken<List<Map<String, Object>>>() {});
        if (maybeList.isPresent()) {
            return maybeList.get();
        }

        throw new FatalConfigurationRuntimeException(key.getDescription() + " is not a map or list of maps");
    }

    public static <T> void checkAssertions(Map<String,?> flags, Map<String, Object> assertions, String target, Supplier<T> actualSupplier) {
        AssertionSupport support = new AssertionSupport();
        checkAssertions(support, flags, assertions, target, actualSupplier);
        support.validate();
    }

    public static <T> void checkAssertions(Map<String,?> flags, List<Map<String, Object>> assertions, String target, Supplier<T> actualSupplier) {
        AssertionSupport support = new AssertionSupport();
        for (Map<String, Object> assertionMap : assertions) {
            checkAssertions(support, flags, assertionMap, target, actualSupplier);
        }
        support.validate();
    }

    public static <T> void checkAssertions(AssertionSupport support, Map<String,?> flags, List<Map<String, Object>> assertions, String target, Supplier<T> actualSupplier) {
        for (Map<String, Object> assertionMap : assertions) {
            checkAssertions(support, flags, assertionMap, target, actualSupplier);
        }
    }

    public static <T> void checkAssertions(AssertionSupport support, Map<String,?> flags, final Map<String, Object> assertions, final String target, final Supplier<T> actualSupplier) {
        if (assertions == null) {
            return;
        }
        try {
            Asserts.succeedsEventually(flags, new Runnable() {
                @Override
                public void run() {
                    T actual = actualSupplier.get();
                    checkActualAgainstAssertions(assertions, target, actual);
                }
            });
        } catch (Throwable t) {
            support.fail(t);
        }
    }

    public static <T> void checkActualAgainstAssertions(AssertionSupport support, Map<String, Object> assertions, String target, T actual) {
        try {
            checkActualAgainstAssertions(assertions, target, actual);
        } catch (Throwable t) {
            support.fail(t);
        }
    }

    public static <T> void checkActualAgainstAssertions(Map<String, Object> assertions, String target, T actual) {
        for (Map.Entry<String, Object> assertion : assertions.entrySet()) {
            Maybe<Condition> condition = Condition.tryFromString(assertion.getKey());
            Object expected = assertion.getValue();

            if (condition.isAbsentOrNull() || !condition.get().check(target, expected)) {
                failAssertion(target, condition.or(Condition.UNKNOWN), expected);
            }
        }
    }

    public static void failAssertion(String target, Condition assertion, Object expected) {
        throw new AssertionError(Joiner.on(' ').useForNull("null").join(target, assertion, expected));
    }

    public static boolean isTrue(Object object) {
        return null != object && Boolean.valueOf(object.toString());
    }

    /**
     * A convenience to collect multiple assertion failures.
     */
    public static class AssertionSupport {
        private List<AssertionError> failures = new ArrayList<>();

        public void fail(String target, Condition assertion, Object expected) {
            failures.add(new AssertionError(Joiner.on(' ').useForNull("null").join(target, assertion, expected)));
        }

        public void fail(Throwable throwable) {
            failures.add(new AssertionError(throwable.getMessage(), throwable));
        }

        /**
         * @throws AssertionError if any failures were collected.
         */
        public void validate() {
            if (failures.size() > 0) {
                if (failures.size() == 1) {
                    throw failures.get(0);
                }
                StringBuilder builder = new StringBuilder();
                for (AssertionError assertionError : failures) {
                    builder.append(assertionError.getMessage()).append("\n");
                }
                throw new AssertionError("Assertions failed:\n" + builder, new CompoundRuntimeException("Assertions", failures));
            }
        }
    }
}
