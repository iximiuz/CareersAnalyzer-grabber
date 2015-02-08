import scrapy
from scrapy.contrib.spiders import CrawlSpider


class JobItem(scrapy.Item):
    id = scrapy.Field()
    source = scrapy.Field()
    name = scrapy.Field()
    url = scrapy.Field()
    published_at = scrapy.Field()
    skills = scrapy.Field()
    company = scrapy.Field()
    location = scrapy.Field()
    relocation = scrapy.Field()


class BaseJobsCrawler(CrawlSpider):
    def __init__(self, jobs_processor, reactor):
        super(BaseJobsCrawler, self).__init__()
        self._jobs_processor = jobs_processor
        self._reactor = reactor

    def process_job(self, job_item):
        self._jobs_processor.process(job_item)

    def start(self):
        self._reactor.run()

    def stop(self):
        self._reactor.stop()