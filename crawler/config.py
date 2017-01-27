from crawler.urls import url_tools

# Info
version = "0.3-dev"
project_url = "https://github.com/UPOLSearch/UPOL-Crawler"
user_agent = "Mozilla/5.0 (compatible; UPOL-Crawler/" + version + "; +" + project_url + ")"

# Settings
limit_domain = "http://upol.cz"
regex = url_tools.generate_regex(limit_domain)
max_value = 5
seed_path = "config/seed.txt"
# for requests
verify_ssl = False
