from job_sources import BaseJobsCrawler, JobItem
from scrapy.http import Request
from scrapy.contrib.spiders import Rule
from scrapy.contrib.linkextractors import LinkExtractor


class SoCareersSpider(BaseJobsCrawler):
    name = 'so-careers'
    allowed_domains = ['careers.stackoverflow.com']
    start_urls = ['http://careers.stackoverflow.com/jobs']
    rules = [Rule(LinkExtractor(allow=['/jobs$']), 'parse_jobs')]

    def __init__(self, jobs_processor, reactor):
        super(SoCareersSpider, self).__init__(jobs_processor, reactor)
        self._page_count = None
        self._cur_page = 1

    def parse_jobs(self, response):
        if self._page_count is None:
            pages = [str(s.extract()) for s in response.xpath('//a[starts-with(@href, "/jobs?pg=")]/text()')]
            self._page_count = max(map(int, [p for p in pages if p.isdigit()]))

        for job_id in response.xpath('//a[@data-jobid]/@data-jobid'):
            job_item = JobItem()
            job_item['id'] = job_id.extract().encode('utf-8')
            job_item['source'] = self.name
            job_node_selector = "//a[@data-jobid='%s']//ancestor::*[contains(@class, '-job')][1]" % job_item['id']
            job_item['name'] = response.xpath(job_node_selector + "//a[starts-with(@href, '/jobs/%s')]/text()"
                                              % job_item['id']).extract()[0]
            job_item['url'] = u'http://careers.stackoverflow.com' + \
                              response.xpath(job_node_selector + "//a[starts-with(@href, '/jobs/%s')]/@href"
                                             % job_item['id']).extract()[0]
            job_item['skills'] = []
            for tag in response.xpath(job_node_selector + '//a[starts-with(@href, "/jobs/tag/")]/text()'):
                job_item['skills'].append(str(tag.extract()))

            self.process_job(job_item)

        if self._cur_page < self._page_count:
            self._cur_page += 1
            return Request(self.start_urls[0] + '?pg=' + str(self._cur_page), callback=self.parse_jobs)

        self.stop()