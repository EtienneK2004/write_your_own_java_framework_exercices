package com.github.forax.framework.mapper;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public final class JSONWriter {

  private interface JSONFunction {
    String apply(JSONWriter jsonWriter, Object instance);
  }

  private static class PropertyClassValue extends ClassValue<List<JSONFunction>> {

    @Override
    protected List<JSONFunction> computeValue(Class<?> type) {
      return Arrays.stream(Utils.beanInfo(type).getPropertyDescriptors())
          .filter(propertyDescriptor -> !"class".equals(propertyDescriptor.getName()))
          .map(propertyDescriptor ->
              (JSONFunction) (JSONWriter writer, Object o) ->
                  '"' + propertyDescriptor.getName() + "\": " + writer.toJSON(Utils.invokeMethod(o, propertyDescriptor.getReadMethod()))
          )
          .toList();
    }
  }

  private static final ClassValue<List<JSONFunction>> CLASS_VALUE = new PropertyClassValue();


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
    var jsonFunctions = CLASS_VALUE.get(o.getClass());
    return jsonFunctions.stream()
        .map(func -> func.apply(this, o))
        .collect(Collectors.joining(", ", "{", "}"));
  }
}
