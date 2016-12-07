from crawler import urls

#TODO - load values from file
blacklist = ["portal.upol.cz",
            "stag.upol.cz",
            "stagservices.upol.cz",
            "courseware.upol.cz",
            "helpdesk.upol.cz"]

def is_url_blocked(url):
    return True if urls.domain(url) in blacklist else False
