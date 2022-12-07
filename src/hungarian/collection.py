import re
from itertools import product
from string import ascii_lowercase

import aswan
import datazimmer as dz
import requests
import urllib3
from bs4 import BeautifulSoup

ea_rex = re.compile("EA=(\d+)")

valtor_base = dz.SourceUrl("https://valtor.valasztas.hu")
jsp_ext = "/valtort/jsp/"
valtor_list = dz.SourceUrl(f"{valtor_base}{jsp_ext}tmd1.jsp?TIP=2")

letter_frame = (
    valtor_base
    + jsp_ext
    + "telkiv.jsp?EA={}&TIP=0&URLTIP=1&URL=szavkorval&URLPAR=URL%3D2%26URLPAR%3D2&CH={}"
)


urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"


class ListHandler(aswan.RequestSoupHandler):
    url_root = valtor_base

    def parse(self, soup: "BeautifulSoup"):
        val_ids = [
            int(ea_rex.findall(a["href"])[0]) for a in soup.find_all("a", href=ea_rex)
        ]
        letter_urls = [
            letter_frame.format(i, l) for i, l in product(val_ids, ascii_lowercase)
        ]

        self.register_links_to_handler(letter_urls, LetterHandler)

    def start_session(self, _: "requests.Session"):
        urllib3.util.ssl_.DEFAULT_CIPHERS = "ALL:@SECLEVEL=1"


class LetterHandler(ListHandler):
    process_indefinitely: bool = True

    def parse(self, soup: "BeautifulSoup"):
        loc_urls = [
            f"{valtor_base}{jsp_ext}" + link["href"]
            for link in soup.find_all("a", {"href": re.compile(".*TAZ=.*")})
        ]
        self.register_links_to_handler(loc_urls, LocHandler)


class LocHandler(LetterHandler):
    def parse(self, soup: "BeautifulSoup"):
        recs = []
        ea = re.findall("EA=(\d+)&", self._url)[0]
        path_root = "szavjkv.jsp" if (int(ea) < 33) else "szavjkv2014.jsp"
        for tr in soup.find_all("table")[2].find_all("tr")[1:]:
            link, locstr, locinfo = tr.find_all("td")
            href = link.find("a")["href"]
            params_str = re.findall("EA=\d+&MAZ=\d+&TAZ=\d+&SORSZ=\d+", href)[0]
            links = [
                f"{valtor_base}{jsp_ext}{path_root}?{params_str}&W={w}" for w in [1, 2]
            ]
            self.register_links_to_handler(links, VoteInstHandler)
            recs.append(
                {"link": href, "loc": locstr.text.strip(), "info": locinfo.text.strip()}
            )
        return recs


class VoteInstHandler(LetterHandler):
    def parse(self, soup: "BeautifulSoup"):
        return soup.decode("utf-8")


class HunElectonCollector(dz.DzAswan):
    name: str = "hun-election"
    starters = {
        ListHandler: [valtor_list],
        LetterHandler: [],
        LocHandler: [],
        VoteInstHandler: [],
    }

    def prepare_run(self):
        self.project.max_cpu_use = 4
