import time
import scrapy

NUMBER_OF_PAGES = 1
EXCLUDE_CATEGORIES = ["Confirmed", "NSFW"]


class MemeSpider(scrapy.Spider):
    name = "meme"
    start_urls = [
        f"https://knowyourmeme.com/memes/popular/page/{page}"
        for page in range(1, NUMBER_OF_PAGES + 1)
    ]

    def parse(self, response):
        for meme in response.css(".entry_list td"):
            # Extract the category of the meme from the labels
            labels = [
                label.css("::text").get().strip()
                for label in meme.css(".entry-labels span")
            ]
            # Remove the categories that we don't want
            labels = [label for label in labels if label not in EXCLUDE_CATEGORIES]

            yield {
                "title": meme.css("h2 a::text").get(),
                "url": meme.css("h2 a::attr(href)").get(),
                "last_update_source": time.time(),
                "category": "Meme" if len(labels) == 0 else labels[0],
                "template_image_url": meme.css("img::attr(data-src)").get(),
            }

        next_page = response.css(".next::attr(href)").get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)
