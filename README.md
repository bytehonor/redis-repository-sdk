# redis

## 自定义starter的命名规范
```
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-thymeleaf</artifactId>
  <version>1.2.2</version>
</dependency>
```

## 官方建议自定义的starter使用xxx-spring-boot-starter命名规则
以区分SpringBoot生态提供的starter

```
<dependency>
  <groupId>org.mybatis.spring.boot</groupId>
  <artifactId>mybatis-spring-boot-starter</artifactId>
  <version>1.2.2</version>
</dependency>
```

redis-starter-sdk

## 项目结构

```
1. 此项目为定义一个redis整合SpringBoot的starter,为了方便区别用my前缀标记自定义的类。
2. myredis-spring-boot-autoconfigure：自定义myredis-starter的核心，核心都在这个module中
3. myredis-spring-boot-starter: 仅仅添加了myredis-spring-boot-autoconfigure的依赖 ，目的是隐藏 细节
4. springboot-demo : 引入自定义的starter依赖,进行测试
```

## 官方项目结构
spring-boot-starter-data-redis

```
plugins {
	id "org.springframework.boot.starter"
}

description = "Starter for using Redis key-value data store with Spring Data Redis and the Lettuce client"

dependencies {
	api(project(":spring-boot-project:spring-boot-starters:spring-boot-starter"))
	api("org.springframework.data:spring-data-redis")
	api("io.lettuce:lettuce-core")
}
```