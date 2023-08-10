import json

import pandas as pd
import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlencode,quote_plus


def should_abort_request(request):
    return (
        request.resource_type == "image" or ".jpg" in request.url or ".woff2" in request.url
    )


class GlassDoor(scrapy.Spider):
    name = "glassdoor_spider"


    def start_requests(self):
        for i, row in enumerate(self.df.itertuples(), start=1):
            url = "https://www.glassdoor.com/Search/results.htm?keyword=" + quote_plus(row.company_name)
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={"row": row}, errback=self.failure, meta={
                "playwright": True,
                # "playwright_context": f"{row.permco}",
                "playwright_context_kwargs": {
                    "java_script_enabled": False,
                    "ignore_https_errors": True,
                    # "proxy": {
                    #     "server": "http://geo.iproyal.com:12321",
                    #     "username": "ehsan",
                    #     "password": "ehsan123123123_streaming-1",
                    # },
                },
            })


    def parse(self, response, **kwargs):
        company = kwargs.get("row").company_name
        for i, result in enumerate(response.xpath("//div/a[@data-test='company-tile']"), start=1):
            title = result.xpath(".//h3/text()").get('')
            if (title.lower() in company.lower()) or (company.lower() in title.lower()):
                url = response.urljoin(result.xpath("./@href").get(''))
                yield scrapy.Request(url, callback=self.parse_company, errback=self.failure, cb_kwargs={"row": kwargs.get("row")}, meta={
                    "playwright": True,
                    # "playwright_context": f"{kwargs.get('row').permco}_{i}",
                    "playwright_context_kwargs": {
                        "java_script_enabled": False,
                        "ignore_https_errors": True,
                        # "proxy": {
                        #     "server": "http://geo.iproyal.com:12321",
                        #     "username": "ehsan",
                        #     "password": "ehsan123123123_streaming-1",
                        # },
                    },
                })
                break


    def parse_company(self, response, **kwargs):
        item = {
            "permco": kwargs.get("row").permco,
            "cusip": kwargs.get("row").cusip,
            "company_name": kwargs.get("row").company_name,
            "city": kwargs.get("row").city,
            "state": kwargs.get("row").state,
            "overall_rating": self.get_rating(response),
            "diversity_rating": self.get_diversity_rating(response),
        }
        if not item.get("diversity_rating"):
            url = response.xpath("//a[@data-test='ei-nav-culture-link']/@href").get()
            if url:
                url = response.urljoin(url)
                yield scrapy.Request(url, callback=self.parse_helper, cb_kwargs={"item": item}, meta={
                    "playwright": True,
                    # "playwright_context": f"{kwargs.get('row').cusip}",
                    "playwright_context_kwargs": {
                        "java_script_enabled": False,
                        "ignore_https_errors": True,
                        # "proxy": {
                        #     "server": "http://geo.iproyal.com:12321",
                        #     "username": "ehsan",
                        #     "password": "ehsan123123123_streaming-1",
                        # },
                    },
                })
        else:
            yield item


    def parse_helper(self, response, item):
        try:
            data = json.loads(response.xpath("//script[@type='application/ld+json']/text()").get())
            diversity_rating = data.get('ratingValue')
            reviews = data.get('ratingCount')
            item['diversity_rating'] = f"{diversity_rating} ({reviews})"
        except Exception as e:
            self.logger.error(e)
        yield item



    def get_rating(self, response):
        rating = response.xpath("//div[@data-test='statsLink']/div/text()").get()
        reviews = response.xpath("//a[@data-test='reviewSeeAllLink']/text()").re_first("\d+")
        return f"{rating} ({reviews})"


    def get_diversity_rating(self, response):
        diversity_rating = response.xpath("//div[@data-test='ratingContainer']/div[@data-test='reviewScoreNumber']/text()").get()
        reviews = response.xpath("//div[@data-test='ratingContainer']/div[@data-test='reviewCount']/text()").re_first("\d+")
        if diversity_rating is None or reviews is None:
            return None
        return f"{diversity_rating} ({reviews})"


    def failure(self, failure):
        self.logger.error(repr(failure))


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(GlassDoor, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider


    def spider_opened(self, spider):
        spider.logger.info(" file opened ")
        self.df = pd.read_excel("firm_names.xlsx")



crawler = CrawlerProcess(settings=dict(
        REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7',
        HTTPCACHE_ENABLED = False,
        USER_AGENT = None,
        DOWNLOAD_HANDLERS={
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        PLAYWRIGHT_BROWSER_TYPE = "firefox",
        PLAYWRIGHT_ABORT_REQUEST = should_abort_request,
        FEED_EXPORTERS={
            'xlsx': 'scrapy_xlsx.XlsxItemExporter',
        },
        FEEDS = {"glassdoor.xlsx": {"format": "xlsx"}},
    )
)
crawler.crawl(GlassDoor)
crawler.start()