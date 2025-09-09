package com.github.forax.framework.mapper;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class JSONWriter {
  private static class PropertyClassValue extends ClassValue<List<Property>> {

    @Override
    protected List<Property> computeValue(Class<?> type) {
      return Arrays.stream(Utils.beanInfo(type).getPropertyDescriptors())
          .filter(propertyDescriptor -> !"class".equals(propertyDescriptor.getName()))
          .map(propertyDescriptor ->
              new Property('"' + propertyDescriptor.getName() + "\": ", propertyDescriptor.getReadMethod()))
          .toList();
    }
  }

  private static final ClassValue<List<Property>> CLASS_VALUE = new PropertyClassValue();

  private record Property(String key, Method method) {}


  public String toJSON(Object o) {
    return switch(o) {
      case null -> "null";
      case Boolean bool -> "" + bool;
      case Integer integer -> "" + integer;
      case Double doub -> "" + doub;
      case String str -> '"' + str + '"';
      default -> beanToJSON(o);
    };
  }

  private String beanToJSON(Object o) {
    var properties = CLASS_VALUE.get(o.getClass());
    return properties.stream()
        .map(property-> property.key() + toJSON(Utils.invokeMethod(o, property.method())))
        .collect(Collectors.joining(", ", "{", "}"));
  }
}
