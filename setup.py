from setuptools import setup, find_packages
import mycluster

version = mycluster.__version__

long_description = """
mycluster is a utility to start and stop ipcluster using SSH.
"""

setup(name='mycluster',
      version=version,
      description='A utility to start and stop ipcluster using SSH',
      long_description=long_description,
      classifiers=[
          "Development Status :: 1 - Planning",
          "Intended Audience :: Developers",
          "Intended Audience :: Science/Research",
          "Programming Language :: Python",
          "Topic :: Scientific/Engineering",
          "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      keywords='ipcluster',
      author=mycluster.__author__,
      author_email='mnishida@hiroshima-u.ac.jp',
      url='http://home.hiroshima-u.ac.jp/mnishida/',
      license=mycluster.__license__,
      packages=find_packages(exclude=['ez_setup']),
      include_package_data=True,
      test_suite='nose.collector',
      test_requires=['Nose'],
      zip_safe=False,
      install_requires=[
          'setuptools',
          # -*- Extra requirements: -*-
          'ipyparallel'
          # 'numpy>=1.7',
          # 'scipy>=0.12',
          # 'ipython>=1.0'
      ],
      entry_points={
          'console_scripts': ['mycluster=mycluster:main'],
          }
)
