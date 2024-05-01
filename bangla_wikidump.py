import requests
from bs4 import BeautifulSoup
from lxml import etree
import mwparserfromhell
from tqdm import tqdm

class WikiDumpDownloader:
    def __init__(self):
        self.downloaded_file_size = 0

    def get_latest_bengali_wiki_dump_url(self):
        url = "https://dumps.wikimedia.org/bnwiki/latest/"
        
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and 'pages-articles.xml.bz2' in href:
                full_url = url + href
                size_in_bytes = self.get_file_size(full_url)
                return full_url, size_in_bytes

    def get_file_size(self, url):
        response = requests.head(url)
        return int(response.headers.get('content-length', 0))

    def download_wikidump(self, url, filename):
        response = requests.get(url, stream=True)

        with open(filename, "wb") as file:
            total_length = int(response.headers.get('content-length'))
            for data in tqdm(iterable=response.iter_content(chunk_size=40*1024), total=total_length//40*1024, unit='KB'):
                file.write(data)
        self.downloaded_file_size = total_length

    def extract_titles(self, xml_file):
        context = etree.iterparse(xml_file, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}title')

        for event, elem in context:
            print(elem.text)
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

    def extract_sections(self, xml_file):
        context = etree.iterparse(xml_file, events=('end',), tag='{http://www.mediawiki.org/xml/export-0.10/}text')

        pc = 0
        for event, elem in context:
            pc += 1
            wikicode = mwparserfromhell.parse(elem.text)
            for section in wikicode.get_sections(levels=[2, 3, 4, 5, 6]):
                title = section.filter_headings()
                if title:
                    print(title[0].title.strip_code().strip())
            
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            if pc > 100:
                break


def main():
    downloader = WikiDumpDownloader()
    dump_url, size_in_bytes = downloader.get_latest_bengali_wiki_dump_url()

    print(f"Bengali Latest Wiki Dump URL: {dump_url}")
    print(f"Size: {size_in_bytes} bytes ({size_in_bytes/10*1024**3} GB)")

    filename = "bnwiki-latest-pages-articles.xml.bz2"

    #downloader.download_wikidump(dump_url, filename)

    #downloader.extract_titles('bnwiki-latest-pages-articles.xml')

    downloader.extract_sections('bnwiki-latest-pages-articles.xml')
    
    print("hey")


if __name__ == '__main__':
    main()
