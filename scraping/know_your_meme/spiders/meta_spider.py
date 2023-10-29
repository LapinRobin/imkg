import scrapy


class MetaSpider(scrapy.Spider):
    name = "meta"
    start_urls = ["https://knowyourmeme.com/memes/philosoraptor"]

    def parse(self, response):
        for meme in response.css(".entry_list td"):
            yield {
                "title": meme.css("h2 a::text").get(),
                "url": meme.css("h2 a::attr(href)").get(),
            }

        next_page = response.css(".next::attr(href)").get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)
