package com.github.forax.framework.orm;

import javax.sql.DataSource;
import java.beans.Introspector;
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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
      } catch (UncheckedSQLException e) {
        connection.rollback();
        throw e.getCause();
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
    var columnNames = properties.stream()
        .map(ORM::findColumnName)
        .toList();
    var idProperty = properties.stream()
        .filter(p -> ORM.hasAnnotation(p, Id.class))
        .findFirst()
        .orElse(null);
    var columnNamesMap = properties.stream()
        .collect(Collectors.toMap(
            PropertyDescriptor::getName,
            ORM::findColumnName
        ));
    InvocationHandler invocationHandler = (Object proxy, Method method, Object[] args) -> {
      try {

        var methodName = method.getName();
        return switch (methodName) {
          case "findAll" -> findAll(tableName, properties, constructor);
          case "save" -> save(args[0], tableName, columnNames, properties, idProperty);
          case "findById" -> findById(tableName, properties, constructor, idProperty, args[0]);
          case "equals", "hashCode", "toString" -> throw new UnsupportedOperationException("Not yet implemented");
          default -> {
            var query = method.getAnnotation(Query.class);
            if(query != null) {
              yield findByQuery(properties, constructor, query.value(), args != null ? args : new Object[0]);
            }

            if (methodName.startsWith("findBy")) {
              var propertyName = Introspector.decapitalize(methodName.substring("findBy".length()));
              var colName = columnNamesMap.get(propertyName);
              if(colName == null) {
                throw new IllegalArgumentException(propertyName + " is not a column.");
              }
              yield findByQuery(properties, constructor,
                  "SELECT * FROM " + tableName + " WHERE " + colName + "=?;", args[0])
                  .stream().findFirst();
            }

            throw new IllegalStateException("Unexpected value: " + methodName);
          }
        };
      } catch (SQLException e) {
        throw new UncheckedSQLException(e);
      }
    };

    return repositoryType.cast(Proxy.newProxyInstance(repositoryType.getClassLoader(), new Class<?>[]{ repositoryType}, invocationHandler));
  }

  static Object save(Object object, String tableName, List<String> columnNames, List<PropertyDescriptor> properties, PropertyDescriptor idProperty) throws SQLException {
    Objects.requireNonNull(object);
    var connection = currentConnection();

    var columns = String.join(", ", columnNames);
    var args = String.join(", ", Collections.nCopies(columnNames.size(), "?"));
    var update = "MERGE INTO " + tableName + " (" + columns + ") VALUES (" + args + ");";

    System.err.println(update);

    try(PreparedStatement statement = connection.prepareStatement(update, Statement.RETURN_GENERATED_KEYS)) {

      for (int i = 0, propertiesSize = properties.size(); i < propertiesSize; i++) {
        var property = properties.get(i);
        var getter = property.getReadMethod();
        var value = Utils.invokeMethod(object, getter);
        statement.setObject(i + 1, value);
      }
      statement.executeUpdate();

      try(var resultSet = statement.getGeneratedKeys()) {
        if (resultSet.next()) {
          var id = resultSet.getObject(1);
          var setter = idProperty.getWriteMethod();
          Utils.invokeMethod(object, setter, id);
        }
      }
    }
    return object;
  }

  private static List<?> findAll(String tableName, List<PropertyDescriptor> properties, Constructor<?> constructor) throws SQLException {
    String sqlQuery = "SELECT * FROM " + tableName + ";";
    return findByQuery(properties, constructor, sqlQuery);
  }

  private static Optional<?> findById(String tableName, List<PropertyDescriptor> properties, Constructor<?> constructor, PropertyDescriptor idProp, Object... args) throws SQLException {
    if (idProp == null) return Optional.empty();
    String sqlQuery = "SELECT * FROM " + tableName + " WHERE " + findColumnName(idProp) + "= ?;";
    return findByQuery(properties, constructor, sqlQuery, args[0]).stream().findFirst();
  }

  private static List<?> findByQuery(List<PropertyDescriptor> properties, Constructor<?> constructor, String sqlQuery, Object... args) throws SQLException {
    var connection = currentConnection();
    try(PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
      for(var i = 0; i < args.length; i++) {
        statement.setObject(i + 1, args[i]);
      }
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
