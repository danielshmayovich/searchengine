package com.handson.searchengine.crawler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.handson.searchengine.kafka.Producer;
import com.handson.searchengine.model.*;
import com.handson.searchengine.util.ElasticSearch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class Crawler {

    @Autowired
    RedisTemplate redisTemplate;

    @Autowired
    ObjectMapper om;

    @Autowired
    Producer producer;

    @Autowired
    ElasticSearch elasticSearch;

    protected final Log logger = LogFactory.getLog(getClass());

    public void crawl(String crawlId, CrawlerRequest crawlerRequest) throws InterruptedException, IOException {
        initCrawlInRedis(crawlId);
        producer.send(CrawlerRecord.of(crawlId,crawlerRequest));

    }

    public void crawlOneRecord(CrawlerRecord rec) throws InterruptedException {
        logger.info("crawling url:" + rec.getUrl());
        try {
            StopReason stopReason = getStopReason(rec);
            setCrawlStatus(rec.getCrawlId(),CrawlStatus.of(rec.getDistance(), rec.getStartTime(), 0, stopReason));            if (stopReason == null) {
                Document webPageContent = Jsoup.connect(rec.getUrl())
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
                        .timeout(5000)
                        .get();
                indexElasticSearch(rec,webPageContent);
                List<String> innerUrls = extractWebPageUrls(rec.getBaseUrl(), webPageContent);
                addUrlsToQueue(rec, innerUrls, rec.getDistance() + 1);
            }
        } catch (HttpStatusException e) {
            logger.error("HTTP error fetching URL: " + rec.getUrl() + " Status: " + e.getStatusCode());
        } catch (IOException e) {
            logger.error("IO error fetching URL: " + rec.getUrl(), e);
        }
        Thread.sleep(1000);
    }

    private StopReason getStopReason(CrawlerRecord rec) {
        if (rec.getDistance() == rec.getMaxDistance() +1) return StopReason.maxDistance;
        if (getVisitedUrls(rec.getCrawlId()) >= rec.getMaxUrls()) return StopReason.maxUrls;
        if (System.currentTimeMillis() >= rec.getMaxTime()) return StopReason.timeout;
        return null;
    }

    private void indexElasticSearch(CrawlerRecord rec, Document webPageContent) {
        logger.info(">> adding elastic search for webPage: " + rec.getUrl());
        String text = String.join(" ", webPageContent.select("a[href]").eachText());
        UrlSearchDoc searchDoc = UrlSearchDoc.of(rec.getCrawlId(), text, rec.getUrl(), rec.getBaseUrl(), rec.getDistance());
        elasticSearch.addData(searchDoc);
    }

    private void addUrlsToQueue(CrawlerRecord rec, List<String> urls, int distance) throws InterruptedException, JsonProcessingException {
        logger.info(">> adding urls to queue: distance->" + distance + " amount->" + urls.size());
        for (String url : urls) {
            if (!crawlHasVisited(rec,url)) {
                producer.send(CrawlerRecord.of(rec).withUrl(url).withIncDistance()) ;
            }
        }
    }

    private List<String> extractWebPageUrls(String baseUrl, Document webPageContent) {
        List<String> links = webPageContent.select("a[href]")
                .eachAttr("abs:href")
                .stream()
                .filter(url -> url.startsWith(baseUrl))
                .collect(Collectors.toList());
        logger.info(">> extracted->" + links.size() + " links");

        return links;
    }
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

}
