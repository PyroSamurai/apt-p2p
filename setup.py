#!/usr/bin/env python

import sys
from distutils.core import setup

setup(
    name = "apt-dht",
    version = "0.0.0",
    author = "Cameron Dale",
    author_email = "<camrdale@gmail.com>",
    url = "http://www.camrdale.org/apt-dht.html",
    license = "GPL",

    packages = ["apt_dht", "apt_dht_Khashmir"],

    scripts = ['apt-dht.py']
    )
