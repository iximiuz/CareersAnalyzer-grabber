from careers_analyzer.job_sources.so_careers import SoCareersSpider
from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy import log
import sqlite3


class JobProcessor:
    def __init__(self, db_conn):
        self._db_conn = db_conn

    def process(self, job):
        self._db_conn.execute("INSERT OR IGNORE INTO job VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                              (job['id'], job['url'], job['name'], None, None, None, None, job['source']))
        job_id = next(self._db_conn.execute('SELECT id FROM job WHERE external_id = ? AND source = ?',
                                            (job['id'], job['source'])))[0]
        if job['skills']:
            self._db_conn.executemany("INSERT OR IGNORE INTO skill VALUES (NULL, ?)",
                                      [(s,) for s in job['skills']])
            select_clause = "SELECT id, name FROM skill WHERE name IN (%s)" % ', '.join(['?']*len(job['skills']))
            skills_by_id = {}
            for row in self._db_conn.execute(select_clause, job['skills']):
                skills_by_id[str(row[1])] = row[0]

            self._db_conn.executemany("INSERT OR IGNORE INTO job_skill (job_id, skill_id) VALUES(?, ?)",
                                      [(job_id, skills_by_id[s],) for s in skills_by_id])

        self._db_conn.commit()


def ensure_schema(db_conn):
    c = db_conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS job (id INTEGER PRIMARY KEY ASC,
                                                 external_id TEXT,
                                                 url TEXT,
                                                 name TEXT,
                                                 published_at TEXT,
                                                 company TEXT,
                                                 location TEXT,
                                                 relocation INTEGER,
                                                 source TEXT,
                                                 UNIQUE (external_id, source))""")
    c.execute("""CREATE TABLE IF NOT EXISTS skill (id INTEGER PRIMARY KEY ASC, name TEXT UNIQUE)""")
    c.execute("""CREATE TABLE IF NOT EXISTS job_skill (job_id INTEGER,
                                                       skill_id INTEGER,
                                                       FOREIGN KEY(job_id) REFERENCES job(id),
                                                       FOREIGN KEY(skill_id) REFERENCES skill(id),
                                                       PRIMARY KEY(job_id, skill_id))""")
    db_conn.commit()


def parse_careers(spider):
    crawler = Crawler(Settings())
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()
    log.start()
    spider.start()


def run(db_path):
    db_conn = sqlite3.connect(db_path)
    ensure_schema(db_conn)
    parse_careers(SoCareersSpider(JobProcessor(db_conn), reactor))
    db_conn.close()