package com.github.forax.framework.injector;

import java.beans.PropertyDescriptor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public final class InjectorRegistry {

  private final HashMap<Class<?>, Supplier<?>> registry = new HashMap<>();

  static List<PropertyDescriptor> findInjectableProperties(Class<?> type) {
    var beanInfo = Utils.beanInfo(type);
    return Arrays.stream(beanInfo.getPropertyDescriptors())
        .filter(propertyDescriptor -> {
              var writeMethod = propertyDescriptor.getWriteMethod();
              return writeMethod != null && writeMethod.isAnnotationPresent(Inject.class);
            })
        .toList();
  }

  public <T> void registerInstance(Class<T> type, T instance){
    Objects.requireNonNull(type, "type is null");
    Objects.requireNonNull(instance, "instance is null");
    registerProvider(type, () -> instance);
  }
  public <T> T lookupInstance(Class<T> type) {
    Objects.requireNonNull(type, "instance is null");
    var supplier = registry.get(type);
    if(supplier == null) {
      throw new IllegalStateException("The type " + type.getName() + " has no instance registered");
    }

    return type.cast(supplier.get());
  }

  public <T> void registerProvider(Class<T> type, Supplier<T> supplier) {
    Objects.requireNonNull(supplier);
    Objects.requireNonNull(type);

    if(registry.putIfAbsent(type, supplier) != null) {
      throw new IllegalStateException("The class " + type.getName() + " is already registered.");
    }
  }

  public <T> void registerProviderClass(Class<T> type, Class<? extends T> providerClass) {
    Objects.requireNonNull(type, "type is null");
    Objects.requireNonNull(providerClass, "providerclass is null");
    var constructor = Arrays.stream(providerClass.getConstructors())
        .filter(c1 -> c1.isAnnotationPresent(Inject.class))
        .reduce((_, _) -> {
          throw new IllegalStateException("Multiple constructors with @Inject annotations");
        }).orElseGet(() -> Utils.defaultConstructor(providerClass));
    var injectableProperties = findInjectableProperties(providerClass);

    registerProvider(type, () -> {
      var parameterTypes = constructor.getParameterTypes();
      var args = Arrays.stream(parameterTypes).map(this::lookupInstance).toArray();
      var obj = Utils.newInstance(constructor, args);
      injectableProperties.forEach(
          propertyDescriptor -> {
            var writeMethod = propertyDescriptor.getWriteMethod();
            var propertyType = propertyDescriptor.getPropertyType();
            Utils.invokeMethod(obj,
                writeMethod,
                lookupInstance(propertyType));
          });
      return type.cast(obj);
    });
  }

  public void registerProviderClass(Class<?> providerClass) {
    Objects.requireNonNull(providerClass, "providerClass is null");
    privateRegisterProviderClass(providerClass);
  }

  private <T> void privateRegisterProviderClass(Class<T> providerClass) {
    registerProviderClass(providerClass, providerClass);
  }
}