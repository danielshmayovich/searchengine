# searchengine


sudo vi /etc/hosts
```
127.0.0.1 kafka
```
### swagger
pom.xml
<br>
<version>2.6.2</version> -> <version>2.5.2</version>
```
		<dependency>
			<groupId>io.springfox</groupId>
			<artifactId>springfox-swagger-ui</artifactId>
			<version>2.6.1</version>
		</dependency><!-- https://mvnrepository.com/artifact/io.springfox/springfox-swagger2 -->
		<dependency>
			<groupId>io.springfox</groupId>
			<artifactId>springfox-swagger2</artifactId>
			<version>2.6.1</version>
		</dependency>
```

config/SwaggerConfig.java
```java
@Configuration
@EnableSwagger2
public class SwaggerConfig {
    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .apis(RequestHandlerSelectors.any())
                .paths(PathSelectors.any())
                .build();
    }
}
```
controller/AppController.java
```java
@RestController
@RequestMapping("/api")
public class AppController {

    @RequestMapping(value = "/hello", method = RequestMethod.GET)
    public String hello(@RequestParam String hello) {
        return "hello";
    }
}
```
http://localhost:8080/swagger-ui.html#
<br>
commit - with swagger
### Algo
```
		<dependency>
			<!-- jsoup HTML parser library @ https://jsoup.org/ -->
			<groupId>org.jsoup</groupId>
			<artifactId>jsoup</artifactId>
			<version>1.13.1</version>
		</dependency>

		<dependency>
			<groupId>joda-time</groupId>
			<artifactId>joda-time</artifactId>
			<version>2.10.13</version>
		</dependency>

		<dependency>
			<groupId>com.fasterxml.jackson.datatype</groupId>
			<artifactId>jackson-datatype-joda</artifactId>
			<version>2.12.3</version>
		</dependency>
```
apply patch - algo
<br>
controller/AppController.java
```java
    private static final int ID_LENGTH = 6;
private Random random = new Random();
@Autowired
    Crawler crawler;

@RequestMapping(value = "/crawl", method = RequestMethod.POST)
public CrawlStatusOut crawl(@RequestBody CrawlerRequest request) throws IOException, InterruptedException {
        String crawlId = generateCrawlId();
        if (!request.getUrl().startsWith("http")) {
        request.setUrl("https://" + request.getUrl());
        }
        CrawlStatus res = crawler.crawl(crawlId, request);
        return CrawlStatusOut.of(res);
        }

private String generateCrawlId() {
        String charPool = "ABCDEFHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < ID_LENGTH; i++) {
        res.append(charPool.charAt(random.nextInt(charPool.length())));
        }
        return res.toString();
        }
```
commit - with algo
### redis
pom.xml
```
		<dependency>
			<groupId>org.springframework.boot</groupId>
			<artifactId>spring-boot-starter-data-redis</artifactId>
		</dependency>
```
application.properties
```
spring.redis.host=redis
spring.redis.port=6379

spring.redis.pool.max-active=8  
spring.redis.pool.max-wait=-1  
spring.redis.pool.max-idle=8  
spring.redis.pool.min-idle=0
```
docker-compose.yml
```
version: '2'
services:
  redis:
    image: redis
    ports:
      - 6379:6379
    privileged: true
```
config/RedisConfig.java
```java
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.text.SimpleDateFormat;

@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String,Object> redisTemplate(RedisConnectionFactory redisConnectionFactory){
        /**
         * Configure your own redisTemplate
         * StringRedisTemplate uses StringRedisSerializer to serialize by default
         * RedisTemplate uses JdkSerializationRedisSerializer to serialize by default
         */
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);

        //Open the default type
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
        objectMapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"));
        Jackson2JsonRedisSerializer jackson2JsonRedisSerializer = new Jackson2JsonRedisSerializer(Object.class);
        jackson2JsonRedisSerializer.setObjectMapper(objectMapper);

        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(new StringRedisSerializer());
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }
}
```
crawler/Crawler.java
```java

    @Autowired
    RedisTemplate redisTemplate;

    @Autowired
    ObjectMapper om;
    
    public void crawl(String crawlId, CrawlerRequest crawlerRequest) throws InterruptedException, IOException {
        initCrawlInRedis(crawlId);
        queue.clear();
        curDistance = 0;
        startTime = System.currentTimeMillis();
        stopReason = null;
        queue.put(CrawlerRecord.of(crawlId, crawlerRequest));
        while (!queue.isEmpty() && getStopReason(queue.peek()) == null) {
            CrawlerRecord rec = queue.poll();
            logger.info("crawling url:" + rec.getUrl());
            setCrawlStatus(rec.getCrawlId(),CrawlStatus.of(rec.getDistance(), rec.getStartTime(), 0, null));
            Document webPageContent = Jsoup.connect(rec.getUrl()).get();
            List<String> innerUrls = extractWebPageUrls(rec.getBaseUrl(), webPageContent);
            addUrlsToQueue(rec, innerUrls, rec.getDistance() +1);
        }
        stopReason = queue.isEmpty() ? null : getStopReason(queue.peek());
        setCrawlStatus(crawlId,CrawlStatus.of(queue.peek().getDistance(), startTime, 0, stopReason));
    }
    
.
.
.
    private void initCrawlInRedis(String crawlId) throws JsonProcessingException {
        setCrawlStatus(crawlId, CrawlStatus.of(0, System.currentTimeMillis(),0,  null));
        redisTemplate.opsForValue().set(crawlId + ".urls.count","1");
    }
    private void setCrawlStatus(String crawlId, CrawlStatus crawlStatus) throws JsonProcessingException {
        redisTemplate.opsForValue().set(crawlId + ".status", om.writeValueAsString(crawlStatus));
    }

    private boolean crawlHasVisited(CrawlerRecord rec, String url) {
        if ( redisTemplate.opsForValue().setIfAbsent(rec.getCrawlId() + ".urls." + url, "1")) {
            redisTemplate.opsForValue().increment(rec.getCrawlId() + ".urls.count",1L);
            return false;
        } else {
            return true;
        }
    }

    private int getVisitedUrls(String crawlId) {
        Object curCount = redisTemplate.opsForValue().get(crawlId + ".urls.count");
        if (curCount == null) return 0;
        return Integer.parseInt(curCount.toString());
    }

    public CrawlStatusOut getCrawlInfo(String crawlId) throws JsonProcessingException {
        CrawlStatus cs = om.readValue(redisTemplate.opsForValue().get(crawlId + ".status").toString(),CrawlStatus.class);
        cs.setNumPages(getVisitedUrls(crawlId));
        return CrawlStatusOut.of(cs);
    }

```
replace status and visited url with redis
<br>
controller/AppController.java
```java

    @RequestMapping(value = "/crawl", method = RequestMethod.POST)
    public String crawl(@RequestBody CrawlerRequest request) throws IOException, InterruptedException {
        String crawlId = generateCrawlId();
        if (!request.getUrl().startsWith("http")) {
            request.setUrl("https://" + request.getUrl());
        }
        new Thread(()-> {
            try {
                crawler.crawl(crawlId, request);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        return crawlId;
    }
    
    @RequestMapping(value = "/crawl/{crawlId}", method = RequestMethod.GET)
    public CrawlStatusOut getCrawl(@PathVariable String crawlId) throws IOException, InterruptedException {
        return crawler.getCrawlInfo(crawlId);
    }
```

commit - with redis

###kafka
pom.xml
```
		<dependency>
			<groupId>org.springframework.kafka</groupId>
			<artifactId>spring-kafka</artifactId>
		</dependency>
```

application.properties
```
spring.kafka.bootstrap-servers=kafka:9092
spring.kafka.producer.retries=0
spring.kafka.producer.acks=1
spring.kafka.producer.batch-size=16384
spring.kafka.producer.properties.linger.ms=0
spring.kafka.producer.buffer-memory = 33554432
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
spring.kafka.consumer.properties.group.id=searchengine
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=true
spring.kafka.consumer.auto-commit-interval=1000
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
spring.kafka.consumer.properties.session.timeout.ms=120000
spring.kafka.consumer.properties.request.timeout.ms=180000
spring.kafka.listener.missing-topics-fatal=false
```
docker-compose.yml
```
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - 2181:2181

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - 9092:9092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```
apply patch kafka
<br>

controller/AppController.java
```java
    @Autowired
    Producer producer;
.
.

    @RequestMapping(value = "/sendKafka", method = RequestMethod.POST)
    public String sendKafka(@RequestBody CrawlerRequest request) throws IOException, InterruptedException {
            producer.send(request);
            return "OK";
    }
```
show offsetExplorer
<br>
commit - with kafka
### kafka - algo

crawler/Crawler.java
```
    remove global variables
    public void crawl(String crawlId, CrawlerRequest crawlerRequest) throws InterruptedException, IOException {
        initCrawlInRedis(crawlId);
        producer.send(CrawlerRecord.of(crawlId, crawlerRequest));
    }

    public void crawlOneRecord(CrawlerRecord rec) throws IOException, InterruptedException {
        logger.info("crawling url:" + rec.getUrl());
        StopReason stopReason = getStopReason(rec);
        setCrawlStatus(rec.getCrawlId(),CrawlStatus.of(rec.getDistance(), rec.getStartTime(), 0, stopReason));
        if (stopReason == null) {
            Document webPageContent = Jsoup.connect(rec.getUrl()).get();
            List<String> innerUrls = extractWebPageUrls(rec.getBaseUrl(), webPageContent);
            addUrlsToQueue(rec, innerUrls, rec.getDistance() +1);
        }
    }
```
kafka/Consumer.java
```java
    @Autowired
    Crawler crawler;

    @Autowired
    ObjectMapper om;

    @KafkaListener(topics = {APP_TOPIC})
    public void listen(ConsumerRecord<?, ?> record) throws IOException, InterruptedException {

        Optional<?> kafkaMessage = Optional.ofNullable(record.value());

        if (kafkaMessage.isPresent()) {

            Object message = kafkaMessage.get();
            crawler.crawlOneRecord(om.readValue(message.toString(), CrawlerRecord.class));
        }
    }
```

commit - with kafka algo
###ElasticSearch
create new index:
```
curl --location --request PUT 'https://avnadmin:1Nljw6Duro7x5mi8@handson-handson-52c9.aivencloud.com:19384/[yourname]'
```
pom.xml
```
		<dependency>
			<groupId>com.squareup.okhttp3</groupId>
			<artifactId>okhttp</artifactId>
			<version>4.8.1</version>
		</dependency>
```
application.properties
```
elasticsearch.base.url=https://avnadmin:1Nljw6Duro7x5mi8@handson-handson-52c9.aivencloud.com:19384
elasticsearch.key=avnadmin:1Nljw6Duro7x5mi8
elasticsearch.index=[yourname]
```
apply patch elasticSearch
<br>
crawler/Crawler.java
```java
    private void indexElasticSearch(CrawlerRecord rec, Document webPageContent) {
        logger.info(">> adding elastic search for webPage: " + rec.getUrl());
        String text = String.join(" ", webPageContent.select("a[href]").eachText());
        UrlSearchDoc searchDoc = UrlSearchDoc.of(rec.getCrawlId(), text, rec.getUrl(), rec.getBaseUrl(), rec.getDistance());
        elasticSearch.addData(searchDoc);
        }
```
https://handson-handson-52c9.aivencloud.com/app/management/opensearch-dashboards/indexPatterns/create
<br>
user: avnadmin
<br>
pass: 1Nljw6Duro7x5mi8
<br>
https://handson-handson-52c9.aivencloud.com/app/discover#
<br>
commit- with elasticsearch
