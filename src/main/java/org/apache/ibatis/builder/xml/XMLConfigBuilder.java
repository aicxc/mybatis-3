/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.builder.xml;

import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;
import javax.sql.DataSource;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.AutoMappingUnknownColumnBehavior;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;

/**
 * 负责把 Mybatis-config.xml 配置文件解析成 Configuration 对象
 *
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

  /**
   * 是否已经解析，用于防止多次（重复）解析
   */
  private boolean parsed;
  /**
   * xpath解析器
   */
  private final XPathParser parser;
  /**
   * 环境id, default = 'development'
   */
  private String environment;
  /**
   * 反射工厂对象
   */
  private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

  public XMLConfigBuilder(Reader reader) {
    this(reader, null, null);
  }

  public XMLConfigBuilder(Reader reader, String environment) {
    this(reader, environment, null);
  }

  public XMLConfigBuilder(Reader reader, String environment, Properties props) {
    this(new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  public XMLConfigBuilder(InputStream inputStream) {
    this(inputStream, null, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment) {
    this(inputStream, environment, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
    this(new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  private XMLConfigBuilder(XPathParser parser, String environment, Properties props) {
    // 实例化 Configuration 对象, 初始化部分类型别名
    super(new Configuration());
    ErrorContext.instance().resource("SQL Mapper Configuration");
    // 设置属性 properties
    this.configuration.setVariables(props);
    this.parsed = false;
    this.environment = environment;
    // new XPathParser(reader, true, props, new XMLMapperEntityResolver()
    this.parser = parser;
  }

  /**
   * 将配置文件 XML 解析成 Configuration 对象
   * @return Configuration instance
   */
  public Configuration parse() {
    // 重复解析则抛出异常
    if (parsed) {
      throw new BuilderException("Each XMLConfigBuilder can only be used once.");
    }
    // 标记已解析
    parsed = true;
    // 解析配置文件 <configuration /> 节点
    parseConfiguration(parser.evalNode("/configuration"));
    return configuration;
  }

  /**
   * 解析 XML 各个节点,参加《XML配置文件》： <a href = http://www.mybatis.org/mybatis-3/zh/configuration.html />
   * @param root root节点，就是 <configuration />节点
   */
  private void parseConfiguration(XNode root) {
    try {
      //issue #117 read properties first
      // 解析 <properties /> 节点
      propertiesElement(root.evalNode("properties"));
      // 解析 <settings /> 节点
      Properties settings = settingsAsProperties(root.evalNode("settings"));
      // 根据配置信息加载自定义VFS实现类
      loadCustomVfs(settings);
      // 根据配置信息加载日志实现类
      loadCustomLogImpl(settings);
      // 解析 <typeAliases /> 节点
      typeAliasesElement(root.evalNode("typeAliases"));
      // 解析 <plugins /> 插件节点
      pluginElement(root.evalNode("plugins"));
      // 解析 <objectFactory /> 对象工厂节点，对象工厂主要用于数据库结果集和POJO之间的转换
      objectFactoryElement(root.evalNode("objectFactory"));
      // 解析 <objectWrapperFactory /> 对象包装工厂
      objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));
      // 解析 <reflectorFactory /> 反射工厂
      reflectorFactoryElement(root.evalNode("reflectorFactory"));
      // 设置 <settings /> 属性到 Configuration 对象中
      settingsElement(settings);
      // read it after objectFactory and objectWrapperFactory issue #631
      // 解析 <environments /> 节点
      environmentsElement(root.evalNode("environments"));
      // 解析 <databaseIdProvider /> 节点
      databaseIdProviderElement(root.evalNode("databaseIdProvider"));
      // 解析 <typeHandlers /> 节点
      typeHandlerElement(root.evalNode("typeHandlers"));
      // 解析 <mappers /> 节点
      mapperElement(root.evalNode("mappers"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
    }
  }

  /**
   * 1.将 <settings /> 节点解析成 Properties 对象
   * 2.通过检验configuration对象里面有没有对应的setter方法验证<setting />子节点的正确性
   *
   * @param context <settings /> 节点
   * @return properties 对象
   */
  private Properties settingsAsProperties(XNode context) {
    if (context == null) {
      return new Properties();
    }
    // 将子节点解析成 properties 对象
    Properties props = context.getChildrenAsProperties();
    // Check that all settings are known to the configuration class
    MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);
    // 验证子节点是否正确
    for (Object key : props.keySet()) {
      if (!metaConfig.hasSetter(String.valueOf(key))) {
        throw new BuilderException("The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
      }
    }
    return props;
  }

  /**
   * 加载自定义VFS实现
   *
   * @param props <settings /> 节点对象 properties
   * @throws ClassNotFoundException Ex
   */
  private void loadCustomVfs(Properties props) throws ClassNotFoundException {
    // <setting name='vfsImpl' value='*class' />
    String value = props.getProperty("vfsImpl");
    if (value != null) {
      // 多个实现以 ',' 分开
      String[] clazzes = value.split(",");
      for (String clazz : clazzes) {
        if (!clazz.isEmpty()) {
          @SuppressWarnings("unchecked")
          Class<? extends VFS> vfsImpl = (Class<? extends VFS>)Resources.classForName(clazz);
          // 设置到 configuration 中
          configuration.setVfsImpl(vfsImpl);
        }
      }
    }
  }

  /**
   * 加载自定义 log 实现
   *
   * @param props <settings ></settings> 转换的 properties 对象
   */
  private void loadCustomLogImpl(Properties props) {
    // <setting name='logImpl' value='*class' />
    Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));
    // 设置 log 实现到 configuration
    configuration.setLogImpl(logImpl);
  }

  /**
   * 解析 <typeAliases /> 标签，将配置类注册到 {@link org.apache.ibatis.type.TypeAliasRegistry} HashMap<String,Class>中。
   * 若已存在相同的 key，则抛出异常
   * @param parent 节点
   */
  private void typeAliasesElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          // <package name='com.pojo.*' />，批量注册别名，别名为 Class.getSimpleName()
          String typeAliasPackage = child.getStringAttribute("name");
          configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);
        } else {
          // 直接注册类和别名，如果别名未配置，则用 Class.getSimpleName()，也就是类名，第一个letter是小写
          String alias = child.getStringAttribute("alias");
          String type = child.getStringAttribute("type");
          try {
            Class<?> clazz = Resources.classForName(type);
            if (alias == null) {
              typeAliasRegistry.registerAlias(clazz);
            } else {
              typeAliasRegistry.registerAlias(alias, clazz);
            }
          } catch (ClassNotFoundException e) {
            throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
          }
        }
      }
    }
  }

  /**
   * 解析 <plugins /> 节点， 添加到 {@link Configuration#interceptorChain}
   *
   * @param parent 节点
   * @throws Exception 异常
   */
  private void pluginElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        // 拦截器 class
        String interceptor = child.getStringAttribute("interceptor");
        // 拦截器 properties
        Properties properties = child.getChildrenAsProperties();
        // 实例化拦截器
        Interceptor interceptorInstance = (Interceptor) resolveClass(interceptor).newInstance();
        // 设置属性
        interceptorInstance.setProperties(properties);
        // 添加到 configuration 的拦截器链中
        configuration.addInterceptor(interceptorInstance);
      }
    }
  }

  /**
   * 用于创建结果对象新实例 {@link org.apache.ibatis.reflection.factory.DefaultObjectFactory}
   *
   * @param context 节点 <objectFactory type="com.*.CustomObjectFactory><property /></objectFactory>
   * @throws Exception 异常
   */
  private void objectFactoryElement(XNode context) throws Exception {
    if (context != null) {
      // 获得 ObjectFactory 的实现类
      String type = context.getStringAttribute("type");
      // 获取 properties
      Properties properties = context.getChildrenAsProperties();
      // 实例化 ObjectFactory
      ObjectFactory factory = (ObjectFactory) resolveClass(type).newInstance();
      // 设置 properties
      factory.setProperties(properties);
      // 设置到 configuration 对象中
      configuration.setObjectFactory(factory);
    }
  }

  /**
   * 对象包装工厂，用于包装Object，很少使用
   *
   * @param context 节点 <objectWrapperFactory type="com.*.CustomObjectWrapperFactory" />
   * @throws Exception 异常
   */
  private void objectWrapperFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      // 实例化
      ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).newInstance();
      // 设置到 configuration 中
      configuration.setObjectWrapperFactory(factory);
    }
  }

  /**
   * 用于支持反射
   *
   * @param context 节点 <reflectorFactory type="com.*.CustomReflectorFactory" />
   * @throws Exception 异常
   */
  private void reflectorFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      // 实例化
      ReflectorFactory factory = (ReflectorFactory) resolveClass(type).newInstance();
      // 设置到 configuration 中
      configuration.setReflectorFactory(factory);
    }
  }

  /**
   * 1.解析 <properties /> 节点，成 Properties 对象
   * 2.优先级：方法传入的properties > 从配置文件resource|url中读取到的属性 > <property />节点
   * 3.将属性properties设置到configuration对象和parser解析器中，供后续使用
   *
   * @param context
   * @throws Exception
   */
  private void propertiesElement(XNode context) throws Exception {
    if (context != null) {
      // 读取 <properties /> 的子节点
      Properties defaults = context.getChildrenAsProperties();
      String resource = context.getStringAttribute("resource");
      String url = context.getStringAttribute("url");
      // resource 和 url 属性不能同时存在
      if (resource != null && url != null) {
        throw new BuilderException("The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
      }
      // 通过 resource 属性读取外部文件 properties
      if (resource != null) {
        defaults.putAll(Resources.getResourceAsProperties(resource));
      }
      // 通过 url 属性读取外部文件 properties
      else if (url != null) {
        defaults.putAll(Resources.getUrlAsProperties(url));
      }
      Properties vars = configuration.getVariables();
      if (vars != null) {
        defaults.putAll(vars);
      }
      // 将 properties 设置到 parser 和 configuration 中
      parser.setVariables(defaults);
      configuration.setVariables(defaults);
    }
  }

  /**
   * 将 <setting /> 节点的配置设置到 configuration
   * 提供缺省值
   *
   * @param props <settings />
   */
  private void settingsElement(Properties props) {
    configuration.setAutoMappingBehavior(AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
    configuration.setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
    configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
    configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
    configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
    configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
    configuration.setMultipleResultSetsEnabled(booleanValueOf(props.getProperty("multipleResultSetsEnabled"), true));
    configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
    configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
    configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
    configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
    configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
    configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
    configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
    configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
    configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
    configuration.setLazyLoadTriggerMethods(stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
    configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
    configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
    configuration.setDefaultEnumTypeHandler(resolveClass(props.getProperty("defaultEnumTypeHandler")));
    configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
    configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
    configuration.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
    configuration.setLogPrefix(props.getProperty("logPrefix"));
    configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
  }

  /**
   * 该节点主要用于配置DB数据源以及事务管理
   *
   * @param context <environments />节点
   * @throws Exception 异常
   */
  private void environmentsElement(XNode context) throws Exception {
    if (context != null) {
      // environment 属性非空，从 default 属性获得
      if (environment == null) {
        environment = context.getStringAttribute("default");
      }
      // 遍历 XNode 节点
      for (XNode child : context.getChildren()) {
        String id = child.getStringAttribute("id");
        // 判断是否默认的environment
        if (isSpecifiedEnvironment(id)) {
          // 解析 `<transactionManager />` 标签，返回 TransactionFactory 对象
          TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));
          // 解析 `<dataSource />` 标签，返回 DataSourceFactory 对象
          DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));
          DataSource dataSource = dsFactory.getDataSource();
          // Builder 设计模式
          Environment.Builder environmentBuilder = new Environment.Builder(id)
              .transactionFactory(txFactory)
              .dataSource(dataSource);
          // 设置 Environment 到 configuration
          configuration.setEnvironment(environmentBuilder.build());
        }
      }
    }
  }

  /**
   * 可将databaseId用于 mapper.xml中，根据不同数据库厂商执行不同的SQL, 增强数据库移植性
   *
   * @param context 节点
   * @throws Exception 异常
   */
  private void databaseIdProviderElement(XNode context) throws Exception {
    DatabaseIdProvider databaseIdProvider = null;
    if (context != null) {
      String type = context.getStringAttribute("type");
      // awful patch to keep backward compatibility
      if ("VENDOR".equals(type)) {
        type = "DB_VENDOR";
      }
      Properties properties = context.getChildrenAsProperties();
      databaseIdProvider = (DatabaseIdProvider) resolveClass(type).newInstance();
      databaseIdProvider.setProperties(properties);
    }
    Environment environment = configuration.getEnvironment();
    if (environment != null && databaseIdProvider != null) {
      String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());
      configuration.setDatabaseId(databaseId);
    }
  }

  /**
   * 通过配置信息实例化TransactionFactory
   *
   */
  private TransactionFactory transactionManagerElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      TransactionFactory factory = (TransactionFactory) resolveClass(type).newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a TransactionFactory.");
  }

  /**
   * 通过配置信息实例化DataSourceFactory
   *
   */
  private DataSourceFactory dataSourceElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      DataSourceFactory factory = (DataSourceFactory) resolveClass(type).newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a DataSourceFactory.");
  }

  /**
   * 类型处理器，负责JavaType和JdbcType之间的转换
   * {@link org.apache.ibatis.type.TypeHandlerRegistry}
   *
   * @param parent 节点
   */
  private void typeHandlerElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        // 如果是 package 节点，则扫描包下的 TypeHandler 并注册
        if ("package".equals(child.getName())) {
          String typeHandlerPackage = child.getStringAttribute("name");
          typeHandlerRegistry.register(typeHandlerPackage);
          // 如果是 typeHandler 节点，则注册该 TypeHandler
        } else {
          String javaTypeName = child.getStringAttribute("javaType");
          String jdbcTypeName = child.getStringAttribute("jdbcType");
          String handlerTypeName = child.getStringAttribute("handler");
          Class<?> javaTypeClass = resolveClass(javaTypeName);
          JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
          Class<?> typeHandlerClass = resolveClass(handlerTypeName);
          // 注册 TypeHandler
          if (javaTypeClass != null) {
            if (jdbcType == null) {
              typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
            } else {
              typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
            }
          } else {
            typeHandlerRegistry.register(typeHandlerClass);
          }
        }
      }
    }
  }

  /**
   * ★ 解析 mappers 节点
   *
   * @param parent mapper 节点
   * @throws Exception 异常
   */
  private void mapperElement(XNode parent) throws Exception {
    if (parent != null) {
      // 遍历子节点
      for (XNode child : parent.getChildren()) {
        // <package name="*" /> 扫描包路径下的 mapper 文件
        if ("package".equals(child.getName())) {
          String mapperPackage = child.getStringAttribute("name");
          // 添加到 configuration 中
          configuration.addMappers(mapperPackage);
        } else {
          // 单个mapper文件的处理，resource、url、class三者只能同时存在一个
          String resource = child.getStringAttribute("resource");
          String url = child.getStringAttribute("url");
          String mapperClass = child.getStringAttribute("class");

          // resource
          if (resource != null && url == null && mapperClass == null) {
            ErrorContext.instance().resource(resource);
            // 读取资源文件流
            InputStream inputStream = Resources.getResourceAsStream(resource);
            // XMLMapperBuilder 解析 mapper.xml
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource, configuration.getSqlFragments());
            mapperParser.parse();

            // url
          } else if (resource == null && url != null && mapperClass == null) {
            ErrorContext.instance().resource(url);
            // 读取资源文件流
            InputStream inputStream = Resources.getUrlAsStream(url);
            // XMLMapperBuilder 解析 mapper.xml
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url, configuration.getSqlFragments());
            mapperParser.parse();

            // Class 解析原理和 package 相同
          } else if (resource == null && url == null && mapperClass != null) {
            Class<?> mapperInterface = Resources.classForName(mapperClass);
            configuration.addMapper(mapperInterface);
          } else {
            throw new BuilderException("A mapper element may only specify a url, resource or class, but not more than one.");
          }
        }
      }
    }
  }

  /**
   * 判断是否default environment id
   * @param id Environment ID
   * @return boolean
   */
  private boolean isSpecifiedEnvironment(String id) {
    if (environment == null) {
      throw new BuilderException("No environment specified.");
    } else if (id == null) {
      throw new BuilderException("Environment requires an id attribute.");
    } else if (environment.equals(id)) {
      return true;
    }
    return false;
  }

}
