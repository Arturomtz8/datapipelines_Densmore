import csv
from urllib.parse import parse_qs, urlsplit

urls = [
    """https://www.mydomain.com/page-name?utm_content=textlink&utm_medium=social
&utm_source=twitter&utm_campaign=fallsale""",
    """https://www.reddit.com/page-name?utm_content=textlink&utm_medium=social
&utm_source=twitter&utm_campaign=fallsale""",
    """https://www.facebook.com/page-name?utm_content=textlink&utm_medium=social
&utm_source=twitter&utm_campaign=fallsale""",
]


all_urls = []

for url in urls:
    parsed_url = []
    split_url = urlsplit(url)
    params = parse_qs(split_url.query)

    # domain
    parsed_url.append(split_url.netloc)

    # url path
    parsed_url.append(split_url.path)

    # utm parameters
    parsed_url.append(params["utm_content"][0])
    parsed_url.append(params["utm_medium"][0])
    parsed_url.append(params["utm_source"][0])
    parsed_url.append(params["utm_campaign"][0])

    all_urls.append(parsed_url)

print(all_urls)
export_file = "export_file.csv"

with open(export_file, "w") as fp:
    csvw = csv.writer(fp, delimiter="|")
    csvw.writerows(all_urls)
