#!/usr/bin/python

import sys
from distutils.core import setup

setup(
    name = "apt-p2p",
    version = "0.1.5",
    author = "Cameron Dale",
    author_email = "<camrdale@gmail.com>",
    url = "http://www.camrdale.org/apt-p2p.html",
    license = "GPL",

    packages = ["apt_p2p", "apt_p2p_Khashmir"],

    scripts = ['apt-p2p.py']
    )
