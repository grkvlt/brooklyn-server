/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.camp.brooklyn.spi.dsl.methods;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampConstants;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.EntityManagerInternal;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.HasSideEffects;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;

public class DslComponent extends BrooklynDslDeferredSupplier<Entity> {

    private static final long serialVersionUID = -7715984495268724954L;
    
    private final String componentId;
    private final DslComponent scopeComponent;
    private final Scope scope;

    public DslComponent(String componentId) {
        this(Scope.GLOBAL, componentId);
    }
    
    public DslComponent(Scope scope, String componentId) {
        this(null, scope, componentId);
    }
    
    public DslComponent(DslComponent scopeComponent, Scope scope, String componentId) {
        Preconditions.checkNotNull(scope, "scope");
        this.scopeComponent = scopeComponent;
        this.componentId = componentId;
        this.scope = scope;
    }

    // ---------------------------
    
    @Override
    public Task<Entity> newTask() {
        return TaskBuilder.<Entity>builder().displayName(toString()).tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
            .body(new EntityInScopeFinder(scopeComponent, scope, componentId)).build();
    }
    
    protected static class EntityInScopeFinder implements Callable<Entity> {
        protected final DslComponent scopeComponent;
        protected final Scope scope;
        protected final String componentId;

        public EntityInScopeFinder(DslComponent scopeComponent, Scope scope, String componentId) {
            this.scopeComponent = scopeComponent;
            this.scope = scope;
            this.componentId = componentId;
        }

        protected EntityInternal getEntity() {
            if (scopeComponent!=null) {
                return (EntityInternal)scopeComponent.get();
            } else {
                return entity();
            }
        }
        
        @Override
        public Entity call() throws Exception {
            Iterable<Entity> entitiesToSearch = null;
            switch (scope) {
                case THIS:
                    return getEntity();
                case PARENT:
                    return getEntity().getParent();
                case GLOBAL:
                    entitiesToSearch = ((EntityManagerInternal)getEntity().getManagementContext().getEntityManager())
                        .getAllEntitiesInApplication( entity().getApplication() );
                    break;
                case ROOT:
                    return getEntity().getApplication();
                case SCOPE_ROOT:
                    return Entities.catalogItemScopeRoot(getEntity());
                case DESCENDANT:
                    entitiesToSearch = Entities.descendants(getEntity());
                    break;
                case ANCESTOR:
                    entitiesToSearch = Entities.ancestors(getEntity());
                    break;
                case SIBLING:
                    entitiesToSearch = getEntity().getParent().getChildren();
                    break;
                case CHILD:
                    entitiesToSearch = getEntity().getChildren();
                    break;
                default:
                    throw new IllegalStateException("Unexpected scope "+scope);
            }
            
            Optional<Entity> result = Iterables.tryFind(entitiesToSearch, EntityPredicates.configEqualTo(BrooklynCampConstants.PLAN_ID, componentId));
            
            if (result.isPresent())
                return result.get();
            
            // TODO may want to block and repeat on new entities joining?
            throw new NoSuchElementException("No entity matching id " + componentId+
                (scope==Scope.GLOBAL ? "" : ", in scope "+scope+" wrt "+getEntity()+
                (scopeComponent!=null ? " ("+scopeComponent+" from "+entity()+")" : "")));
        }        
    }
    
    // -------------------------------

    // DSL words which move to a new component
    
    public DslComponent entity(String scopeOrId) {
        return new DslComponent(this, Scope.GLOBAL, scopeOrId);
    }
    public DslComponent child(String scopeOrId) {
        return new DslComponent(this, Scope.CHILD, scopeOrId);
    }
    public DslComponent sibling(String scopeOrId) {
        return new DslComponent(this, Scope.SIBLING, scopeOrId);
    }
    public DslComponent descendant(String scopeOrId) {
        return new DslComponent(this, Scope.DESCENDANT, scopeOrId);
    }
    public DslComponent ancestor(String scopeOrId) {
        return new DslComponent(this, Scope.ANCESTOR, scopeOrId);
    }
    public DslComponent root() {
        return new DslComponent(this, Scope.ROOT, "");
    }
    public DslComponent scopeRoot() {
        return new DslComponent(this, Scope.SCOPE_ROOT, "");
    }
    
    @Deprecated /** @deprecated since 0.7.0 */
    public DslComponent component(String scopeOrId) {
        return new DslComponent(this, Scope.GLOBAL, scopeOrId);
    }
    
    public DslComponent parent() {
        return new DslComponent(this, Scope.PARENT, "");
    }
    
    public DslComponent component(String scope, String id) {
        if (!DslComponent.Scope.isValid(scope)) {
            throw new IllegalArgumentException(scope + " is not a vlaid scope");
        }
        return new DslComponent(this, DslComponent.Scope.fromString(scope), id);
    }

    // DSL words which return things
    
    public BrooklynDslDeferredSupplier<?> attributeWhenReady(final String sensorName) {
        return new AttributeWhenReady(this, sensorName);
    }
    // class simply makes the memento XML files nicer
    protected static class AttributeWhenReady extends BrooklynDslDeferredSupplier<Object> {
        private static final long serialVersionUID = 1740899524088902383L;
        private final DslComponent component;
        private final String sensorName;
        public AttributeWhenReady(DslComponent component, String sensorName) {
            this.component = Preconditions.checkNotNull(component);
            this.sensorName = sensorName;
        }
        @SuppressWarnings("unchecked")
        @Override
        public Task<Object> newTask() {
            Entity targetEntity = component.get();
            Sensor<?> targetSensor = targetEntity.getEntityType().getSensor(sensorName);
            if (!(targetSensor instanceof AttributeSensor<?>)) {
                targetSensor = Sensors.newSensor(Object.class, sensorName);
            }
            return (Task<Object>) DependentConfiguration.attributeWhenReady(targetEntity, (AttributeSensor<?>)targetSensor);
        }
        @Override
        public int hashCode() {
            return Objects.hashCode(component, sensorName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            AttributeWhenReady that = AttributeWhenReady.class.cast(obj);
            return Objects.equal(this.component, that.component) &&
                    Objects.equal(this.sensorName, that.sensorName);
        }
        @Override
        public String toString() {
            return (component.scope==Scope.THIS ? "" : component.toString()+".") +
                "attributeWhenReady("+JavaStringEscapes.wrapJavaString(sensorName)+")";
        }
    }

    public BrooklynDslDeferredSupplier<?> effector(final String effectorName) {
        return new ExecuteEffector(this, effectorName, ImmutableMap.<String, Object>of());
    }
    public BrooklynDslDeferredSupplier<?> effector(final String effectorName, final Map<String, ?> args) {
        return new ExecuteEffector(this, effectorName, args);
    }
    public BrooklynDslDeferredSupplier<?> effector(final String effectorName, Object... args) {
        return new ExecuteEffector(this, effectorName, ImmutableList.copyOf(args));
    }

    // class simply makes the memento XML files nicer
    protected static class ExecuteEffector extends BrooklynDslDeferredSupplier<Object> implements HasSideEffects {
        private static final long serialVersionUID = 1740899524088902383L;
        private final DslComponent component;
        private final String effectorName;
        private final Map<String, ?> args;
        private final List<? extends Object> argList;
        private Task<?> cachedTask;
        public ExecuteEffector(DslComponent component, String effectorName, Map<String, ?> args) {
            this.component = Preconditions.checkNotNull(component);
            this.effectorName = effectorName;
            this.args = args;
            this.argList = null;
        }
        public ExecuteEffector(DslComponent component, String effectorName, List<? extends Object> args) {
            this.component = Preconditions.checkNotNull(component);
            this.effectorName = effectorName;
            this.argList = args;
            this.args = null;
        }
        @SuppressWarnings("unchecked")
        @Override
        public Task<Object> newTask() {
            Entity targetEntity = component.get();
            Maybe<Effector<?>> targetEffector = targetEntity.getEntityType().getEffectorByName(effectorName);
            if (targetEffector.isAbsentOrNull()) {
                throw new IllegalArgumentException("Effector " + effectorName + " not found on entity: " + targetEntity);
            }
            if (null == cachedTask) {
                cachedTask = null == argList
                    ? Entities.invokeEffector(targetEntity, targetEntity, targetEffector.get(), args)
                    : invokeWithDeferredArgs(targetEntity, targetEffector.get(), argList);
//                    : Entities.invokeEffectorWithArgs(targetEntity, targetEntity, targetEffector.get(), argList.toArray());
            }
            return (Task<Object>) cachedTask;
        }

        public static Task<Object> invokeWithDeferredArgs(final Entity targetEntity, final Effector<?> targetEffector, final List<? extends Object> args) {
            List<TaskAdaptable<Object>> taskArgs = Lists.newArrayList();
            for (Object arg: args) {
                if (arg instanceof TaskAdaptable) taskArgs.add((TaskAdaptable<Object>)arg);
                else if (arg instanceof TaskFactory) taskArgs.add( ((TaskFactory<TaskAdaptable<Object>>)arg).newTask() );
            }
                
            return DependentConfiguration.transformMultiple(
                MutableMap.<String,String>of("displayName", "invoking '"+targetEffector.getName()+"' with "+taskArgs.size()+" task"+(taskArgs.size()!=1?"s":"")), 
                    new Function<List<Object>, Object>() {
                @Override public Object apply(List<Object> input) {
                    Iterator<?> tri = input.iterator();
                    Object[] vv = new Object[args.size()];
                    int i=0;
                    for (Object arg : args) {
                        if (arg instanceof TaskAdaptable || arg instanceof TaskFactory) vv[i] = tri.next();
                        else if (arg instanceof DeferredSupplier) vv[i] = ((DeferredSupplier<?>) arg).get();
                        else vv[i] = arg;
                        i++;
                    }
                    
                    return Entities.invokeEffectorWithArgs(targetEntity, targetEntity, targetEffector, vv);
                }},
                taskArgs);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(component, effectorName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ExecuteEffector that = ExecuteEffector.class.cast(obj);
            return Objects.equal(this.component, that.component) &&
                    Objects.equal(this.effectorName, that.effectorName);
        }
        @Override
        public String toString() {
            return (component.scope==Scope.THIS ? "" : component.toString()+".") +
                "effector("+JavaStringEscapes.wrapJavaString(effectorName)+")";
        }
    }

    public BrooklynDslDeferredSupplier<?> config(final String keyName) {
        return new DslConfigSupplier(this, keyName);
    }
    protected final static class DslConfigSupplier extends BrooklynDslDeferredSupplier<Object> {
        private final DslComponent component;
        private final String keyName;
        private static final long serialVersionUID = -4735177561947722511L;

        public DslConfigSupplier(DslComponent component, String keyName) {
            this.component = Preconditions.checkNotNull(component);
            this.keyName = keyName;
        }

        @Override
        public Task<Object> newTask() {
            return Tasks.builder().displayName("retrieving config for "+keyName).tag(BrooklynTaskTags.TRANSIENT_TASK_TAG).dynamic(false).body(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    Entity targetEntity = component.get();
                    return targetEntity.getConfig(ConfigKeys.newConfigKey(Object.class, keyName));
                }
            }).build();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(component, keyName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslConfigSupplier that = DslConfigSupplier.class.cast(obj);
            return Objects.equal(this.component, that.component) &&
                    Objects.equal(this.keyName, that.keyName);
        }

        @Override
        public String toString() {
            return (component.scope==Scope.THIS ? "" : component.toString()+".") + 
                "config("+JavaStringEscapes.wrapJavaString(keyName)+")";
        }
    }
    
    public BrooklynDslDeferredSupplier<Sensor<?>> sensor(final String sensorName) {
        return new DslSensorSupplier(this, sensorName);
    }
    protected final static class DslSensorSupplier extends BrooklynDslDeferredSupplier<Sensor<?>> {
        private final DslComponent component;
        private final String sensorName;
        private static final long serialVersionUID = -4735177561947722511L;

        public DslSensorSupplier(DslComponent component, String sensorName) {
            this.component = Preconditions.checkNotNull(component);
            this.sensorName = sensorName;
        }

        @Override
        public Task<Sensor<?>> newTask() {
            return Tasks.<Sensor<?>>builder().displayName("looking up sensor for "+sensorName).dynamic(false).body(new Callable<Sensor<?>>() {
                @Override
                public Sensor<?> call() throws Exception {
                    Entity targetEntity = component.get();
                    Sensor<?> result = null;
                    if (targetEntity!=null) {
                        result = targetEntity.getEntityType().getSensor(sensorName);
                    }
                    if (result!=null) return result;
                    return Sensors.newSensor(Object.class, sensorName);
                }
            }).build();
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(component, sensorName);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            DslSensorSupplier that = DslSensorSupplier.class.cast(obj);
            return Objects.equal(this.component, that.component) &&
                    Objects.equal(this.sensorName, that.sensorName);
        }

        @Override
        public String toString() {
            return (component.scope==Scope.THIS ? "" : component.toString()+".") + 
                "sensor("+JavaStringEscapes.wrapJavaString(sensorName)+")";
        }
    }

    public static enum Scope {
        GLOBAL ("global"),
        CHILD ("child"),
        PARENT ("parent"),
        SIBLING ("sibling"),
        DESCENDANT ("descendant"),
        ANCESTOR("ancestor"),
        ROOT("root"),
        SCOPE_ROOT("scopeRoot"),
        THIS ("this");
        
        public static final Set<Scope> VALUES = ImmutableSet.of(GLOBAL, CHILD, PARENT, SIBLING, DESCENDANT, ANCESTOR, ROOT, SCOPE_ROOT, THIS);
        
        private final String name;
        
        private Scope(String name) {
            this.name = name;
        }
        
        public static Scope fromString(String name) {
            return tryFromString(name).get();
        }
        
        public static Maybe<Scope> tryFromString(String name) {
            for (Scope scope : VALUES)
                if (scope.name.toLowerCase().equals(name.toLowerCase()))
                    return Maybe.of(scope);
            return Maybe.absent(new IllegalArgumentException(name + " is not a valid scope"));
        }
        
        public static boolean isValid(String name) {
            for (Scope scope : VALUES)
                if (scope.name.toLowerCase().equals(name.toLowerCase()))
                    return true;
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(componentId, scopeComponent, scope);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        DslComponent that = DslComponent.class.cast(obj);
        return Objects.equal(this.componentId, that.componentId) &&
                Objects.equal(this.scopeComponent, that.scopeComponent) &&
                Objects.equal(this.scope, that.scope);
    }

    @Override
    public String toString() {
        return "$brooklyn:entity("+
            (scopeComponent==null ? "" : JavaStringEscapes.wrapJavaString(scopeComponent.toString())+", ")+
            (scope==Scope.GLOBAL ? "" : JavaStringEscapes.wrapJavaString(scope.toString())+", ")+
            JavaStringEscapes.wrapJavaString(componentId)+
            ")";
    }

}