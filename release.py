# Converts README.md to README.txt (in restructured text format), builds package, and uploads to PyPI

import pypandoc
import os

rst = pypandoc.convert('README.md', 'rst')
f = open('README.txt','w+')
f.write(rst)
f.close()
os.system("python3 setup.py register sdist upload")
os.remove('README.txt')

