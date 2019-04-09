from setuptools import setup, find_packages
from distutils.core import setup

setup(
	name='elasticsearch-autocomplete',
	version='0.1',
	description='',
	url='https://github.com/saksham0751/myproject',
	licence='MIT',
	packages=['elasticsearch-autocomplete'],
	#package_dir={'abc':'src/abc'},
	#package_data={'abc': ['data/*.dat']},
	install_requires=['elasticsearch'],
     )
