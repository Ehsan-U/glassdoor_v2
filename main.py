import json
import pandas as pd
import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlencode,quote_plus
from playwright.async_api import async_playwright



def should_abort_request(request):
    return (
        request.resource_type == "image" or ".jpg" in request.url or ".woff2" in request.url
    )


class GlassDoor(scrapy.Spider):
    name = "glassdoor_spider"


    def start_requests(self):
        for i, row in enumerate(self.df.itertuples(), start=1):
            url = "https://www.glassdoor.com/Search/results.htm?keyword=" + quote_plus(row.company_name)
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={"company": row.company_name}, errback=self.failure, meta={
                "playwright": True,
                "playwright_context": f"{row.permco}_{i}",
                "playwright_include_page": True,
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
            if i == 10:
                break


    async def parse(self, response, **kwargs):
        page = response.meta.get("playwright_page")
        await page.close()
        await page.context.close()
        company = kwargs.get("company")
        for i, result in enumerate(response.xpath("//div/a[@data-test='company-tile']"), start=1):
            title = result.xpath(".//h3/text()").get('')
            if (title.lower() in company.lower()) or (company.lower() in title.lower()):
                url = response.urljoin(result.xpath("./@href").get(''))
                yield scrapy.Request(url, callback=self.parse_company, cb_kwargs={"company": company}, errback=self.failure, meta={
                    "playwright": True,
                    "playwright_context": f"{company}_{i}",
                    "playwright_include_page": True,
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


    async def parse_company(self, response, company):
        page = response.meta.get("playwright_page")
        await page.close()
        await page.context.close()
        item = {
            "overall_rating": self.get_rating(response),
            "diversity_rating": self.get_diversity_rating(response),
            "company": company
        }
        if not item.get("diversity_rating"):
            url = response.xpath("//a[@data-test='ei-nav-culture-link']/@href").get()
            if url:
                url = response.urljoin(url)
                item['diversity_rating'] = await self.parse_helper(url)
        yield item


    async def parse_helper(self, url):
        try:
            async with async_playwright() as p:
                browser = await p.firefox.launch(headless=True)
                context = await browser.new_context(java_script_enabled=False, ignore_https_errors=True)
                page = await context.new_page()
                await page.goto(url)
                content = await page.content()
                response = scrapy.Selector(text=content)
                await browser.close()
            data = json.loads(response.xpath("//script[@type='application/ld+json']/text()").get())
            diversity_rating = data.get('ratingValue')
            reviews = data.get('ratingCount')
            return f"{diversity_rating} ({reviews})"
        except Exception as e:
            self.logger.error(e)


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


    async def failure(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
            await page.context.close()


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
        PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 4,
        PLAYWRIGHT_MAX_CONTEXTS = 8,
        PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60 * 1000
    )
)
crawler.crawl(GlassDoor)
crawler.start()