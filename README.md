# elasticsearch-spring-boot-starter
Elasticsearch RestHighLevelClient客户端通用查询实现。

### quick start
  1.依赖编译
  ```shell script
    git clone https://github.com/WangNian007/commons.git
    cd commons
    mvn install

    git clone https://github.com/WangNian007/elasticsearch-spring-boot-starter.git
    cd elasticsearch-spring-boot-starter
    mvn install
  ```
  2.依赖引入
  ```xml
   <dependency>
     <groupId>com.wangnian</groupId>
     <artifactId>elasticsearch-spring-boot-starter</artifactId>
     <version>1.0-SNAPSHOT</version>
   </dependency>
  ```
  3.添加配置(yaml配置)
  ```yaml
    spring.es:
      username: 
      password: 
      hosts:
        - http://127.0.0.1:9200
        - http://127.0.0.2:9200
  ```
  4.使用连接池
  ```yaml
    spring.es:
      pool:
        maxTotal: 10
  ```
  5.通用查询
  ```java
  ```
### 功能丰富
  1.支持绝大部分场景下的查询。
  ~~2.高亮，聚合，热力，地理相关查询等。~~
  3.可根据ES版本切换API,只需在pom文件中修改对应版本即可(7.17版本之前)
### Q&A
  欢迎使用，有问题随时沟通！！！