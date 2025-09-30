package com.github.forax.framework.orm;

import javax.sql.DataSource;
import java.beans.PropertyDescriptor;
import java.io.Serial;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ORM {
  private ORM() {
    throw new AssertionError();
  }

  @FunctionalInterface
  public interface TransactionBlock {
    void run() throws SQLException;
  }

  private static final Map<Class<?>, String> TYPE_MAPPING = Map.of(
      int.class, "INTEGER",
      Integer.class, "INTEGER",
      long.class, "BIGINT",
      Long.class, "BIGINT",
      String.class, "VARCHAR(255)"
  );

  private static Class<?> findBeanTypeFromRepository(Class<?> repositoryType) {
    var repositorySupertype = Arrays.stream(repositoryType.getGenericInterfaces())
        .flatMap(superInterface -> {
          if (superInterface instanceof ParameterizedType parameterizedType
              && parameterizedType.getRawType() == Repository.class) {
            return Stream.of(parameterizedType);
          }
          return null;
        })
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("invalid repository interface " + repositoryType.getName()));
    var typeArgument = repositorySupertype.getActualTypeArguments()[0];
    if (typeArgument instanceof Class<?> beanType) {
      return beanType;
    }
    throw new IllegalArgumentException("invalid type argument " + typeArgument + " for repository interface " + repositoryType.getName());
  }

  private static class UncheckedSQLException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 42L;

    private UncheckedSQLException(SQLException cause) {
      super(cause);
    }

    @Override
    public SQLException getCause() {
      return (SQLException) super.getCause();
    }
  }


  // --- do not change the code above

  // Q1 Transaction

  private static final ThreadLocal<Connection> CONNECTION_THREAD_LOCAL = new ThreadLocal<>();

  public static void transaction(DataSource dataSource, TransactionBlock transactionBlock) throws SQLException {
    Objects.requireNonNull(dataSource);
    Objects.requireNonNull(transactionBlock);
    try(Connection connection = dataSource.getConnection()) {
      CONNECTION_THREAD_LOCAL.set(connection);
      connection.setAutoCommit(false);
      try {
        transactionBlock.run();
      } catch(SQLException e) {
        connection.rollback();
        throw e;
      } finally {
        CONNECTION_THREAD_LOCAL.remove();
      }
      connection.commit();
    }
  }

  /*Package private */
  static Connection currentConnection() {
    var conn = CONNECTION_THREAD_LOCAL.get();
    if(conn == null) {
      throw new IllegalStateException("No connection available");
    }
    return conn;
  }

  // Create table

  static String findTableName(Class<?> beanClass) {
    var annotation = beanClass.getAnnotation(Table.class);
    if (annotation != null) {
      return annotation.value();
    }
    return beanClass.getSimpleName().toUpperCase(Locale.ROOT);
  }

  static String findColumnName(PropertyDescriptor property) {
    var getter = property.getReadMethod();
    if (getter != null) {
      var annotation = getter.getAnnotation(Column.class);
      if (annotation != null) {
        return annotation.value();
      }
    }
    return property.getName();
  }

  static boolean hasAnnotation(PropertyDescriptor property, Class<? extends Annotation> annotation) {
    var getter = property.getReadMethod();
    return getter != null && getter.isAnnotationPresent(annotation);
  }

  static String getSQLForProperty(PropertyDescriptor p) {
    var colName = findColumnName(p);
    var propType = p.getPropertyType();
    var typeName = TYPE_MAPPING.get(propType);
    if (typeName == null) {
      throw new IllegalStateException("Invalid property type " + propType.getName());
    }
    var notNull = propType.isPrimitive();
    var autoIncrement = hasAnnotation(p, GeneratedValue.class);
    var primaryKey = hasAnnotation(p, Id.class);
    return colName + " " + typeName
        + (notNull ? " NOT NULL" : "")
        + (autoIncrement ? " AUTO_INCREMENT" : "")
        + (primaryKey ? ",\nPRIMARY KEY (" + colName + ")" : "");
  }

  public static void createTable(Class<?> beanClass) throws SQLException {
    Objects.requireNonNull(beanClass);
    var conn = currentConnection();
    var beanInfo = Utils.beanInfo(beanClass);
    var columns = Arrays.stream(beanInfo.getPropertyDescriptors())
        .filter(p -> !p.getName().equals("class"))
        .map(ORM::getSQLForProperty)
        .collect(Collectors.joining(",\n"));

    var tableName = findTableName(beanClass);
    var update = "CREATE TABLE " + tableName + "(\n" + columns + ")";

    System.err.println(update);

    try(var statement = conn.createStatement()) {
      statement.executeUpdate(update);
    }
    conn.commit();
  }


  public static <R extends Repository<?, ?>> R createRepository(Class<R> repositoryType) {
    Objects.requireNonNull(repositoryType);
    var beanType = findBeanTypeFromRepository(repositoryType);
    var tableName = findTableName(beanType);
    var beanInfo = Utils.beanInfo(beanType);
    var properties = Arrays.stream(beanInfo.getPropertyDescriptors())
        .filter(p -> !p.getName().equals("class"))
        .toList();
    var constructor = Utils.defaultConstructor(beanType);
    InvocationHandler invocationHandler = (Object proxy, Method method, Object[] args) ->
        switch(method.getName()) {
          case "findAll" -> findAll(tableName, properties, constructor);
          case "equals", "hashCode", "toString" -> throw new UnsupportedOperationException("Not yet implemented");
          default -> throw new IllegalStateException("Unexpected value: " + method.getName());
        };

    return repositoryType.cast(Proxy.newProxyInstance(repositoryType.getClassLoader(), new Class<?>[]{ repositoryType}, invocationHandler));
  }

  private static List<?> findAll(String tableName, List<PropertyDescriptor> properties, Constructor<?> constructor) throws SQLException {
    String sqlQuery = "SELECT * FROM " + tableName + ";";
    var connection = currentConnection();
    try(PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
      try(ResultSet resultSet = statement.executeQuery()) {
        var list = new ArrayList<Object>();
        while(resultSet.next()) {
          var object = Utils.newInstance(constructor);

          for (int i = 0, propertiesSize = properties.size(); i < propertiesSize; i++) {
            var prop = properties.get(i);
            var value = resultSet.getObject(i + 1);
            var setter = prop.getWriteMethod();

            Utils.invokeMethod(object, setter, value);
          }
          list.add(object);
        }

        return list;
      }
    }
  }

  //TODO
}
