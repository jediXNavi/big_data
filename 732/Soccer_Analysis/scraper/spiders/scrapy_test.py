import scrapy,csv
from numpy import unicode
from scrapy.exporters import CsvItemExporter
from scraper.Mongo_Provider import MongoProvider


class PlayerSpider(scrapy.Spider):


    name = "spidy"
    start_urls = ['https://sofifa.com/players?showCol%5B0%5D=pi&showCol%5B1%5D=ae&showCol%5B2%5D=hi&showCol%5B3%5D=wi&showCol%5B4%5D=pf&showCol%5B5%5D=oa&showCol%5B6%5D=pt&showCol%5B7%5D=bo&showCol%5B8%5D=bp&showCol%5B9%5D=gu&showCol%5B10%5D=jt&showCol%5B11%5D=le&showCol%5B12%5D=vl&showCol%5B13%5D=wg&showCol%5B14%5D=rc&showCol%5B15%5D=ta&showCol%5B16%5D=cr&showCol%5B17%5D=fi&showCol%5B18%5D=he&showCol%5B19%5D=sh&showCol%5B20%5D=vo&showCol%5B21%5D=ts&showCol%5B22%5D=dr&showCol%5B23%5D=cu&showCol%5B24%5D=fr&showCol%5B25%5D=lo&showCol%5B26%5D=bl&showCol%5B27%5D=to&showCol%5B28%5D=ac&showCol%5B29%5D=sp&showCol%5B30%5D=ag&showCol%5B31%5D=re&showCol%5B32%5D=ba&showCol%5B33%5D=tp&showCol%5B34%5D=so&showCol%5B35%5D=ju&showCol%5B36%5D=st&showCol%5B37%5D=sr&showCol%5B38%5D=ln&showCol%5B39%5D=te&showCol%5B40%5D=ar&showCol%5B41%5D=in&showCol%5B42%5D=po&showCol%5B43%5D=vi&showCol%5B44%5D=pe&showCol%5B45%5D=cm&showCol%5B46%5D=td&showCol%5B47%5D=ma&showCol%5B48%5D=sa&showCol%5B49%5D=sl&showCol%5B50%5D=tg&showCol%5B51%5D=gd&showCol%5B52%5D=gh&showCol%5B53%5D=gk&showCol%5B54%5D=gp&showCol%5B55%5D=gr&showCol%5B56%5D=tt&showCol%5B57%5D=bs&showCol%5B58%5D=wk&showCol%5B59%5D=sk&showCol%5B60%5D=aw&showCol%5B61%5D=dw&showCol%5B62%5D=ir&showCol%5B63%5D=pac&showCol%5B64%5D=sho&showCol%5B65%5D=pas&showCol%5B66%5D=dri&showCol%5B67%5D=def&showCol%5B68%5D=phy&offset=0']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        kwargs['mongo_uri'] = crawler.settings.get("MONGO_URI")
        kwargs['mongo_database'] = crawler.settings.get('MONGO_DATABASE')
        return super(PlayerSpider, cls).from_crawler(crawler, *args, **kwargs)

    def __init__(self, mongo_uri=None, mongo_database=None, **kwargs):
        super().__init__(**kwargs)
        self.infile = open("output.csv", "w", newline="")
        self.app_file = open("output.csv","a", newline="")

        self.mongo_provider = MongoProvider(mongo_uri, mongo_database)
        self.collection = self.mongo_provider.get_collection()
        print(mongo_uri)
        print(mongo_database)
        self.exporter = CsvItemExporter(self.infile, unicode)
        self.exporter.start_exporting()
        self.iter = 0

    def parse(self, response):
        headers = []
        player_att = []

        if response.status == 200:
            if self.iter == 0:
                header = response.css('table.table-hover tr.persist-header')
                for i in range(1,72):
                    head = header.css('th')[i].css('::text').extract_first()
                    headers.append(head)
                    # writer = csv.writer(self.infile)
                # writer.writerow(headers)
                self.iter += 1
            #Populating the rows
            rows = response.css('table.table-hover tbody tr')
            # print(rows)
            for j in range(0,60):
                for i in range(1,72):
                    selector = 'div a::text' if i in (1,5) else '::text'
                    each = rows[j].css('td')[i].css(selector).extract_first()
                    print(each)
                    player_att.append(each)
                writer = csv.writer(self.app_file)
                writer.writerow(player_att)
                player_att = []
            results = response.css('div.pagination a.bp3-button span.bp3-button-text::text').extract()
            for index, result in enumerate(results):
                if result == 'Next':
                    next_page = response.css('div.pagination a::attr(href)')[index].get()
                    if next_page:
                        next_href = response.css('div.pagination a::attr(href)').get()
                        #next_page_url = 'https://sofifa.com' + next_href
                        next_page_url = response.urljoin(next_page)
                        print(next_page_url)
                        yield scrapy.Request(url=next_page_url, callback=self.parse)


        # rows = row.css('td')[i].css('::text').extract_first()
        #teams = response.xpath("//span[@class='hidden-xs']/text()").extract()
        # goals = response.css('a.fs13 span::text').get()
        # rows = response.css('table.with-centered-columns')
        # print(rows)
        # for row in rows:
        #     yield{
        #     'teams':row.css('td.fs13 a.fwn span.hidden-xs::text').extract(),
        #     'goals':row.css('a.fs13 span::text').get()
        #     }
        #
        #
        # for q in response.css("article.contentslim"):
        #     name = q.css("h1::text").extract_first()
        #     link = q.css("p a::attr(href)").extract_first()
        #     yield {'Name':name,'Link':link}
        #
        #     writer = csv.writer(self.infile)
        #     writer.writerow([name,link])

    def my_callback(self,response):
        print("callback called")