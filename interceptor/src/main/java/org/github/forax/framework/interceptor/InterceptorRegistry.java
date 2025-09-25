package org.github.forax.framework.interceptor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class InterceptorRegistry {
  /*private final HashMap<Class<? extends Annotation>, List<AroundAdvice>> adviceMap = new HashMap<>();

  public void addAroundAdvice(Class<? extends Annotation> annotation, AroundAdvice aroundAdvice) {
    Objects.requireNonNull(annotation);
    Objects.requireNonNull(aroundAdvice);

    adviceMap.computeIfAbsent(annotation, _ -> new ArrayList<>()).add(aroundAdvice);
  }

  List<AroundAdvice> findAdvices(Method method) {
    return Stream.of(method.getAnnotations())
        .flatMap(annotation -> {
          var annotationType = annotation.annotationType();
          var advices = adviceMap.getOrDefault(annotationType, List.of());
          return advices.stream();
        })
        .toList();
  }

  @SuppressWarnings("BoundedWildcard")
  public <T> T createProxy(Class<T> interfaceType, T delegate) {
    Objects.requireNonNull(interfaceType);
    Objects.requireNonNull(delegate);

    if(!interfaceType.isInterface()) {
      throw new IllegalArgumentException("interfaceType must be an interface");
    }

    return interfaceType.cast(Proxy.newProxyInstance(
        interfaceType.getClassLoader(),
        new Class<?>[] { interfaceType },
        (_, method, args) -> {
          var advices = findAdvices(method);
          for(var advice : advices) {
            advice.before(delegate, method, args);
          }
          Object result = null;
          try {
            result = Utils.invokeMethod(delegate, method, args);
          } finally {
            for(var advice : advices.reversed()) {
              advice.after(delegate, method, args, result);
            }
            return result;
          }
        }));
  }*/


  private final HashMap<Class<? extends Annotation>, List<Interceptor>> interceptorMap = new HashMap<>();


  @SuppressWarnings({"BoundedWildcard", "RedundantSuppression"})
  public <T> T createProxy(Class<T> interfaceType, T delegate) {
    Objects.requireNonNull(interfaceType);
    Objects.requireNonNull(delegate);

    if(!interfaceType.isInterface()) {
      throw new IllegalArgumentException("interfaceType must be an interface");
    }

    return interfaceType.cast(Proxy.newProxyInstance(
        interfaceType.getClassLoader(),
        new Class<?>[] { interfaceType },
        (_, method, args) -> {
          var invocation = getInvocation(findInterceptors(method));

          return invocation.proceed(delegate, method, args);
        }));
  }

  Invocation getInvocation(List<Interceptor> interceptors) {
    Invocation invocation = Utils::invokeMethod;

    for (var interceptor : interceptors.reversed()) {

      var invocCopy = invocation;

      invocation = (o, m, a) -> interceptor.intercept(o, m, a, invocCopy);
    }

    return invocation;
  }

  /*
  Interceptor findInterceptor(Method method) {
    var annotations = method.getAnnotations();
    Interceptor interceptor = (o, _, a, _) -> Utils.invokeMethod(o, method, a);

    for (var annotation : Arrays.asList(annotations).reversed()) {
      var annotationInterceptor = interceptorMap.get(annotation.annotationType());
      if (annotationInterceptor == null) {
        continue;
      }
      
      var interCopy = interceptor;

      interceptor = (o, m, a, invoke) ->
          annotationInterceptor.intercept(o, m, a,
              (o2, m2, a2) -> interCopy.intercept(o2, m2, a2, invoke));
    }
    
    return interceptor;
  }*/

  List<Interceptor> findInterceptors(Method method) {
    var annotations = method.getAnnotations();
    return Stream.of(annotations)
        .flatMap(a -> {
          var aClass = a.getClass();
          var interceptors = interceptorMap.getOrDefault(aClass, List.of());
          return interceptors.stream();
        }).toList();
  }


  public void addInterceptor(Class<? extends Annotation> annotationClass, Interceptor interceptor) {
    Objects.requireNonNull(annotationClass);
    Objects.requireNonNull(interceptor);

    interceptorMap.computeIfAbsent(annotationClass, _ -> new ArrayList<>()).add(interceptor);
  }

  public void addAroundAdvice(Class<? extends Annotation> annotation, AroundAdvice aroundAdvice) {
    Objects.requireNonNull(annotation);
    Objects.requireNonNull(aroundAdvice);

    addInterceptor(annotation, (o, m, a, i) -> {
      aroundAdvice.before(o, m, a);
      Object result = null;
      try {
        result = i.proceed(o, m, a);
      } finally {
        aroundAdvice.after(o, m, a, result);
        return result;
      }
    });
  }
}


