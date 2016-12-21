from crawler.urls import url_tools

# Info
version = "0.3-dev"
project_url = "https://github.com/UPOLSearch/UPOL-Crawler"
user_agent = "Mozilla/5.0 (compatible; UPOL-Crawler/" + version + "; +" + project_url + ")"

# Settings
limit_domain = "http://inf.upol.cz"
regex = url_tools.generate_regex(limit_domain)
max_value = 20
# for requests
verify_ssl = False
