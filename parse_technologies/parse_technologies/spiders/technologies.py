import scrapy
from technology_dict import technology_dict

class TechnologiesSpider(scrapy.Spider):
    name = "technologies"
    allowed_domains = ["djinni.co"]

    @staticmethod
    def clean_technologies(technologies: list[str]) -> set[str]:
        technologies_clean = []
        for technology in technologies:
            if "/" not in technology:
                [technologies_clean.append(key) for key, value in technology_dict.items() if technology in value]
            else:
                technologies_sub_list = [tech_str.strip() for tech_str in technology.split("/")]
                for tech in technologies_sub_list:
                    for key, values in technology_dict.items():
                        if tech in values:
                            technologies_clean.append(key)
        return list(set(technologies_clean))

    def start_requests(self):
        urls = ["https://djinni.co/jobs/?primary_keyword=Python"]
        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.get_positions_links,
            )

    def get_positions_links(self, response):
        links = response.css("a.h3::attr(href)").getall()
        pagination_links = response.css("a.page-link::attr(href)").getall()
        next_page = pagination_links[-1]
        for link in links:
            yield response.follow(
                link,
                callback=self.parse_position,
            )
        if next_page != "#":
            url = response.urljoin(next_page)
            yield response.follow(url, callback=self.get_positions_links)

    def parse_position(self, response):
        position_name = response.css("h1::text").get()

        technologies_rough = response.css(
            "div.job-additional-info--item-text > "
            "span:not([class='location-text'])::text"
            ).getall()

        technologies_clean = self.clean_technologies(technologies_rough)

        experience = response.css(
            "div.job-additional-info--item-text::text"
        ).re(r"\d{1}")[0]
        views = self.clean_views(response.css("p.text-muted::text").getall())
        yield {
            "position": position_name.strip(),
            "technologies": technologies_clean,
            "experience": int(experience) or 0,
            "link": response.url,
            "views": views
        }

    @staticmethod
    def clean_views(views: list):
        views = views[-1].strip().split()
        views = int(views[0])
        return views
