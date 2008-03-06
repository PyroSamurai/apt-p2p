#!/usr/bin/env python

import sys
from distutils.core import setup

from debian_bundle import changelog

f = open('debian/changelog')
data = f.read()
f.close()
c = changelog.Changelog(file=data, max_blocks=1)
del data

setup(
    name = "apt-p2p",
    version = c.full_version,
    author = "Cameron Dale",
    author_email = "<camrdale@gmail.com>",
    url = "http://www.camrdale.org/apt-p2p.html",
    license = "GPL",

    packages = ["apt_p2p", "apt_p2p_Khashmir"],

    scripts = ['apt-p2p.py']
    )
